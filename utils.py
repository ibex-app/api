from celery import Celery
from app.config.constants import CeleryConstants as CC
from app.util.model_utils import deserialize_from_base64
from uuid import UUID

def terminate_monitor_tasks(monitor_id: UUID):
    my_app = Celery("ibex tasks",
                    broker=CC.LOCAL_BROKER_URL_REDIS,
                    backend=CC.LOCAL_RESULT_BACKEND_REDIS,
                    include=[
                        'app.core.celery.tasks.collect', 
                        'app.core.celery.tasks.download', 
                        'app.core.celery.tasks.process']
                    )


    i = my_app.control.inspect()
    scheduled = i.scheduled()
    reserved = i.reserved()
    active = i.active()

    active_tasks = list(active.items())[0][1]
    reserved_tasks = list(reserved.items())[0][1]
    scheduled_tasks = list(scheduled.items())[0][1]

    all_tasks = active_tasks + reserved_tasks + scheduled_tasks

    ids_to_kill = []
    for task in all_tasks:
        first_sub_task = deserialize_from_base64(task['kwargs']['it'][0])
        if first_sub_task.monitor_id == monitor_id:
            ids_to_kill.append(task['id'])

    my_app.control.revoke(ids_to_kill, terminate=True, signal='SIGUSR1')
    # len(active_tasks), len(reserved_tasks), len(scheduled_tasks), len(ids_to_kill)
    print(f'{len(ids_to_kill)} tasks terminated')
    