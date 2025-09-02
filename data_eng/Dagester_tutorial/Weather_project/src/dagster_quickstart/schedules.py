from dagster import ScheduleDefinition
from .jobs import weather_job  # assuming you defined a job in jobs.py

weather_schedule = ScheduleDefinition(
    job=weather_job,                 # ✅ job to run
    cron_schedule="*/5 * * * *",     # ✅ every 5 minutes
    # automation_name="weather_schedule",  # required
)
