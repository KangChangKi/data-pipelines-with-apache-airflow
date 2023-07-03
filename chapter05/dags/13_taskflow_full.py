import uuid
import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator

# start -----> fetch_sales --> clean_sales    -----> join_datasets --> train_model --> deploy_model
#        |--> fetch_weather --> clean_weather --|

with DAG(
    dag_id="13_taskflow_full",
    start_date=airflow.utils.dates.days_ago(3),  # <-- 현재 시점(now) ~ 3 days ago 의 window 를 설정함.
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")   # <-- 반드시 DummyOperator 를 사용해야 함.

    fetch_sales = DummyOperator(task_id="fetch_sales")
    clean_sales = DummyOperator(task_id="clean_sales")

    fetch_weather = DummyOperator(task_id="fetch_weather")
    clean_weather = DummyOperator(task_id="clean_weather")

    join_datasets = DummyOperator(task_id="join_datasets")   # <-- 반드시 DummyOperator 를 사용해야 함.

    start >> [fetch_sales, fetch_weather]     # <-- DummyOperator 로 fan-out 을 만듦. 보기 편하게 만드는 목적.
    fetch_sales >> clean_sales         # <-- fetch_sales 와 clean_sales 가 순사적으로 실행됨.
    fetch_weather >> clean_weather     # <-- `fetch_sales >> clean_sales` 와 `fetch_weather >> clean_weather` 가 병렬로 실행됨.
    [clean_sales, clean_weather] >> join_datasets      # <-- DummyOperator 로 fan-in 을 만듦. (trigger_rule 설정의 대안)
                                                       # dependencies 중 하나의 failure 로 전체 DAG 가 실행이 멈추는 것을 방지하는 목적.
    @task
    def train_model():            # <-- Taskflow API 사용. 내부적으로 PythonOperator 로 변환된다.
        model_id = str(uuid.uuid4())
        return model_id   # <-- 현재 task 의 리턴 값은 내부적으로 XCom 을 통해서 저장되어 다음 task 의 parameter 로 전달된다.

    @task
    def deploy_model(model_id: str):   # <-- 이전 task 의 리턴 값은 내부적으로 XCom 을 통해서 저장되어 현재 task 의 parameter 로 전달된다.
        print(f"Deploying model {model_id}")

    model_id = train_model()
    deploy_model(model_id)

    join_datasets >> model_id  # <-- Taskflow 스타일과 일반 테스트 사이의 의존성으로 두 유형을 혼합.
                               #     첫번째 task 의 리턴 값을 task group 으로 보고 연결시키는 방식. (직관적이지 않다)
