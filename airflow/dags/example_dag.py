from airflow import DAG
from airflow.decorators import task
#from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import random


users = ["alice", "bob", "carol"]

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

    
    # for user in users:
    #     #user_data = fetch_user.override(task_id=f"fetch_{user}")(user)
    #     #user_class = classify_user.override(task_id=f"classify_{user}")(user_data)

    #     hs = high_score_process.override(task_id=f"high_score_{user}")(user_data)
    #     ls = low_score_process.override(task_id=f"low_score_{user}")(user_data)

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
    def classify_user(score):
        score = random.randint(0, 100)
        
        if score >= 50:
            return "high_score"
        return "low_score"

    @task
    def high_score_process(): #needs to call classify_user to get the score and user data
        user = fetch_user.output
        score = classify_user.output
        print(f"Congrats {user}! You have a high score: {score}")

    @task
    def low_score_process():  #needs to call classify_user to get the score and user data
        user = fetch_user.output
        score = classify_user.output
        print(f"{user} has a low score: {score} better luck next time!")


    @task
    def end():
        user = fetch_user.output
        EmptyOperator(task_id=f"end_{user}", trigger_rule=TriggerRule.NONE_FAILED)

        
    start >> classify_user >> [high_score_process, low_score_process] >> end
        #branch >> ls >> end
