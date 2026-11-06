"""Valida los DAG de Airflow (Semana 3): que importen, tengan >= 5 tasks, dependencias y retries.

Se ejecuta en el job 'dag' del CI (tiene Airflow instalado). En un entorno sin Airflow
(por ejemplo, el job normal de pytest) las pruebas se omiten solas.
"""
import os
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAGS_FOLDER = Path(os.getenv("DAGS_FOLDER", Path(__file__).resolve().parents[1] / "airflow" / "dags"))
MIN_TASKS = 5


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def test_los_dags_se_importan_sin_errores(dagbag):
    assert dagbag.import_errors == {}, f"Errores al importar DAGs: {dagbag.import_errors}"


def test_existe_al_menos_un_dag(dagbag):
    assert len(dagbag.dags) >= 1, f"No se encontró ningún DAG en {DAGS_FOLDER}"


def test_cada_dag_tiene_cinco_o_mas_tasks(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert len(dag.tasks) >= MIN_TASKS, f"{dag_id} tiene {len(dag.tasks)} tasks (mínimo {MIN_TASKS})"


def test_todas_las_tasks_tienen_dependencias(dagbag):
    """Ninguna task puede quedar aislada: debe tener tarea anterior o posterior."""
    for dag_id, dag in dagbag.dags.items():
        aisladas = [t.task_id for t in dag.tasks if not t.upstream_task_ids and not t.downstream_task_ids]
        assert not aisladas, f"{dag_id}: tasks sin dependencias: {aisladas}"


def test_todas_las_tasks_tienen_retries(dagbag):
    for dag_id, dag in dagbag.dags.items():
        sin_retries = [t.task_id for t in dag.tasks if (t.retries or 0) < 1]
        assert not sin_retries, f"{dag_id}: tasks sin retries: {sin_retries}"
