from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import random

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="complex_dynamic_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["complex"]
) as dag:

    users = ["alice", "bob", "carol"]

    @task
    def start():
        return "Begin processing"

    @task
    def fetch_user(user):
        # Simulate fetching user name from API 
        user = user.capitalize()
        #     raise ValueError(f"Failed to fetch data for {user}")
        return {"user": user}

    @task.branch
    def classify_user():
        score = random.randint(0, 100)
        
        if score >= 50:
            return "high_score"
        return "low score"

    # def branch_func(classification: str):
    #     return f"{classification}"

    @task
    def high_score_process(data):
        print(f"Congrats {data['user']}! You have a high score: {data['score']}")

    @task
    def low_score_process(data):
        print(f"{data['user']} has a low score: {data['score']}")

    @task
    def cleanup(user):
        print(f"Cleanup done for {user}")

    for user in users:
        user_data = fetch_user.override(task_id=f"fetch_{user}")(user)
        #user_class = classify_user.override(task_id=f"classify_{user}")(user_data)
        branch = BranchPythonOperator(
            task_id=f"branch_{user}",
            python_callable=classify_user,
            op_args=[user_class],
        )

        hs = high_score.override(task_id=f"high_score_{user}")(user_data)
        ls = low_score.override(task_id=f"low_score_{user}")(user_data)

        end = cleanup.override(task_id=f"cleanup_{user}")(user)

        user_data >> user_class >> branch
        
        branch >> hs >> end
        branch >> ls >> end
