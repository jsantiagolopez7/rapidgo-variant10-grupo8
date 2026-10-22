# Diseño del topic `rapidgo.g8.inventory.events`

**Estado:** propuesta preliminar  
**Decisión a documentar:** clave `store_id` con 3 particiones, comparada contra clave `sku` con 6 particiones.

## 1. Contexto

El topic transporta eventos de inventario (altas, bajas, ajustes, reservas) de RapidGo. La **clave del mensaje** y el **número de particiones** son las dos decisiones más difíciles de revertir en Kafka:

- La clave define a qué partición va cada evento (`hash(key) mod nº_particiones`, partitioner por defecto).
- Kafka solo garantiza orden **dentro de una partición**. Misma clave implica misma partición e implica orden.
- El paralelismo máximo de un consumer group es igual al nº de particiones.
- Si se cambia el nº de particiones después, la correspondencia clave → partición cambia y se rompe el orden histórico por clave.

## 2. Alternativas

| | **A (propuesta)** | **B (comparación)** |
|---|---|---|
| Clave | `store_id` | `sku` |
| Particiones | 3 | 6 |
| Factor de replicación (dev) | 1 | 1 |

## 3. Comparación

| Criterio | A: `store_id` / 3 part. | B: `sku` / 6 part. |
|---|---|---|
| **Orden garantizado** | Todos los eventos de una tienda, en orden | Todos los eventos de un SKU (en todas las tiendas), en orden |
| **Orden por (tienda, SKU)** | Sí (es un subconjunto de la tienda) | Sí (es un subconjunto del SKU) |
| **Cardinalidad de la clave** | Baja (nº de tiendas) | Alta (nº de productos) |
| **Distribución entre particiones** | Riesgo de desbalance: con pocas tiendas, 3 particiones pueden quedar muy desiguales y una tienda grande genera una partición caliente | Normalmente más uniforme, salvo SKUs de altísima rotación |
| **Paralelismo máximo de consumo** | 3 consumidores / 3 tareas Spark | 6 consumidores / 6 tareas Spark |
| **Agregados por tienda** (ej. stock de la tienda X) | Naturales: no requieren shuffle entre particiones | Requieren `groupBy store_id` (shuffle en Spark) |
| **Agregados por SKU** (ej. stock global del SKU) | Requieren `groupBy sku` (shuffle) | Naturales |
| **Escalabilidad futura** | Limitada por nº de tiendas y por 3 particiones | Mayor margen |
| **Costo operativo** | Menor (menos particiones, menos archivos y menos recursos del broker) | Algo mayor; irrelevante a esta escala |

### Qué significa en Spark
El source de Kafka en Spark Structured Streaming asigna, por defecto, una partición de Kafka a una partición de Spark. Con 3 particiones, un stage de lectura usa como máximo 3 tareas en paralelo; con 6, hasta 6 (la opción `minPartitions` permite dividir más, pero sin conservar el orden por clave).

## 4. Criterio de decisión

Elegir **A (`store_id` / 3)** si:
- Las consultas y el procesamiento principales son **por tienda** (stock por tienda, reposición, alertas por sucursal).
- Hay suficientes tiendas y de tamaño parecido, de modo que 3 particiones queden razonablemente balanceadas.
- El volumen es bajo o medio y 3 consumidores alcanzan.

Elegir **B (`sku` / 6)** si:
- El procesamiento principal es **por producto** (stock global, rotación, precios).
- Hay pocas tiendas o muy desiguales en volumen (riesgo de partición caliente con A).
- Se prevé más volumen o más consumidores.

**Alternativa C (a evaluar):** clave compuesta `store_id:sku`. Conserva el orden por (tienda, SKU), que suele ser el orden que realmente importa para el stock, y reparte mejor la carga que A. Se pierde el orden total por tienda y por SKU.

## 5. Experimento para decidir con datos

Los resultados de esta sección **no están medidos todavía**: se llenan al ejecutar los comandos con datos representativos de RapidGo.

### 5.1 Crear el topic de comparación (B)
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic rapidgo.g8.inventory.events.sku6 \
  --partitions 6 --replication-factor 1
```

### 5.2 Producir mensajes con clave
Formato `clave:valor` (el separador `:` va aparte del contenido del mensaje).

Topic A, clave `store_id`:
```bash
docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic rapidgo.g8.inventory.events \
  --property parse.key=true --property key.separator='|'
# ejemplo de líneas:
# S01|{"store_id":"S01","sku":"SKU-100","delta":-2}
# S02|{"store_id":"S02","sku":"SKU-100","delta":5}
```

Topic B, clave `sku`:
```bash
docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic rapidgo.g8.inventory.events.sku6 \
  --property parse.key=true --property key.separator='|'
# SKU-100|{"store_id":"S01","sku":"SKU-100","delta":-2}
```

### 5.3 Medir distribución por partición
Offset final de cada partición (cuántos mensajes recibió cada una):
```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic rapidgo.g8.inventory.events
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic rapidgo.g8.inventory.events.sku6
```
Salida con formato `topic:partición:offset`.

### 5.4 Verificar el orden por clave
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic rapidgo.g8.inventory.events \
  --from-beginning --property print.key=true --property print.partition=true
```
Comprobar que todos los mensajes de una misma clave aparecen siempre en la misma partición y en el orden en que se enviaron.

### 5.5 Tabla de resultados (completar)

| Métrica | A: `store_id` / 3 | B: `sku` / 6 |
|---|---|---|
| Mensajes totales | | |
| Mensajes por partición (P0, P1, P2…) | | |
| Partición más cargada / más ligera (ratio) | | |
| Nº de claves distintas usadas | | |
| Orden por clave verificado (sí/no) | | |
| Tareas Spark en paralelo | 3 | 6 |

## 6. Contrato del mensaje (propuesto)

- **Clave:** `store_id` (string, UTF-8). No cambiar el formato de la clave después de producir datos.
- **Valor:** JSON con al menos `event_id`, `event_type`, `store_id`, `sku`, `delta`, `event_ts`.
- Incluir siempre `store_id` y `sku` en el valor, aunque uno de los dos sea la clave, para poder reparticionar sin perder información.

## 7. Decisión

> _Pendiente de completar tras el experimento._
>
> - Opción elegida: …
> - Motivo principal: …
> - Fecha / responsables: …

## 8. Riesgos y notas

- Este entorno es de desarrollo: 1 broker, replicación 1, sin tolerancia a fallos. En producción usar replicación 3 y `min.insync.replicas=2`.
- Aumentar particiones después es posible, pero rompe el mapeo clave → partición. Reducirlas no es posible: hay que crear un topic nuevo y migrar.
- Con muy pocas claves distintas, el hash puede mandar varias a la misma partición y dejar otras vacías. Medirlo con 5.3 antes de decidir.
