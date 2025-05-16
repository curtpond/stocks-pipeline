from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from stock_pipeline import stock_pipeline

# Create a deployment with a daily schedule
deployment = Deployment.build_from_flow(
    flow=stock_pipeline,
    name="stock-pipeline-daily",
    schedule=CronSchedule(cron="0 0 * * *"),  # Run daily at midnight
    tags=["stocks"]
)

if __name__ == "__main__":
    deployment.apply()
