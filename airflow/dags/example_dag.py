from airflow import DAG
from airflow.decorators import task
#from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.empty import EmptyOperator
#from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from logging import getLogger
import random

logger = getLogger(__name__)



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
    def fetch_user(users):
        # Simulate fetching user name from API
        try: 
            user = random.choice(users)
        except Exception:
            logger.error("Failed to fetch data") 
        return user

    @task
    def fetch_score():
        # Simulate fetching score from API
        try:
            scores = random.randint(0, 100)
        except Exception:
            logger.error("Failed to fetch data") 
        return scores

    @task.branch
    def classify_user(scores):
        
        if scores >= 50:
            return "high_score"
        return "low_score"

    @task
    def high_score(user, scores): #needs to call classify_user to get the score and user data
        logger.info(f"Congrats {user}! You have a high score: {scores}")
        #print(f"Congrats {user}! You have a high score: {score}")

    @task
    def low_score(user, scores):  #needs to call classify_user to get the score and user data
        logger.info(f"{user} has a low score: {scores} better luck next time!")
        #print(f"{user} has a low score: {score} better luck next time!")


    start = EmptyOperator(
        task_id="start")
    
    user = fetch_user(users)
    score = fetch_score()
    
    high = EmptyOperator(
        task_id="high_score")
    low = EmptyOperator(
        task_id="low_score")
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed"
    )
    @task
    def cleanup():
        logger.info("Cleaning up resources")
        # Perform any necessary cleanup tasks here

    start >> user >> score >> classify_user >> [high, low] >> end >> cleanup
