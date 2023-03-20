from datetime import timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task(task_id='fetch_reddit_posts')
def fetch_posts_task():
    return DockerOperator(
        # Replace with your Docker image name and tag
        image='robo_mongo_reddit:robo_mongo_reddit', 
        api_version='auto',
        auto_remove=True,
    )

@dag(
    dag_id='reddit_post_fetcher',
    default_args=default_args,
    description='Fetch Reddit posts and store them in MongoDB every hour',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(0),
    catchup=False,
)
def reddit_post_fetcher():
    fetch_posts = fetch_posts_task()

reddit_post_fetcher_dag = reddit_post_fetcher()
