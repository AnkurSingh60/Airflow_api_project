from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator


def decide_flow(**context):
    checkout_amount = context["params"]["checkout_amount"]

    if checkout_amount < 100:
        return "checkout"
    else:
        return "merged"


with DAG(
    dag_id="pipeline_layout",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    params={"checkout_amount":50},
    catchup=False,
    tags=["pipeline", "branching"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    product_page = BashOperator(
        task_id="product_page",
        bash_command='echo "This is the product page task"',
    )

    user_login_page = BashOperator(
        task_id="user_login_page",
        bash_command='echo "This is the user login page task"',
    )

    checkout_page = BashOperator(
        task_id="checkout_page",
        bash_command='echo "This is the checkout page task"',
    )

    real_now = BranchPythonOperator(
        task_id="real_now",
        python_callable=decide_flow,
    )

    checkout = BashOperator(
        task_id="checkout",
        bash_command='echo "This is the checkout task"',
        retries=2,
        retry_delay=timedelta(minutes=1),
    )

    merged = BashOperator(
        task_id="merged",
        bash_command='echo "This is the merged task"',
    )

    alarming_notification = BashOperator(
        task_id="alarming_notification",
        bash_command='echo "This is the alarming notification"',
         trigger_rule="none_failed_min_one_success" ,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> [product_page, user_login_page, checkout_page]

    [product_page, user_login_page, checkout_page] >> real_now

    real_now >> [checkout, merged]

    [checkout, merged] >> alarming_notification

    alarming_notification >> end
