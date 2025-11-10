# src/celery_app/celery_app.py
from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "fastcite_tasks",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["celery_app.tasks"],  # 👈 THIS LINE IS CRUCIAL
)

celery_app.conf.update(
    # Queue routing: assign tasks to specific queues
    task_routes={
        # High-priority chatbot tasks → 'chatbot' queue
        'tasks.search_similar_in_books': {'queue': 'chatbot'},
        'tasks.select_top_contexts': {'queue': 'chatbot'},
        'tasks.call_model': {'queue': 'chatbot'},
        'tasks.process_contexts_and_generate': {'queue': 'chatbot'},
        
        # Low-priority upload tasks → 'uploads' queue
        'process_pdf_pipeline_task': {'queue': 'uploads'},
        'process_pdf_to_qdrant_task': {'queue': 'uploads'},
        'extract_pdf_chunks_task': {'queue': 'uploads'},
        'upload_chunks_to_b2_task': {'queue': 'uploads'},
        'generate_embeddings_task': {'queue': 'uploads'},
        'store_vectors_in_qdrant_task': {'queue': 'uploads'},
        'initialize_qdrant_collection_task': {'queue': 'uploads'},
        'extract_pdf_metadata_task': {'queue': 'uploads'},
        'check_or_create_book_task': {'queue': 'uploads'},
        'update_book_status_task': {'queue': 'uploads'},
        
        # Medium-priority delete tasks → 'maintenance' queue
        'delete_qdrant_chunks_task': {'queue': 'maintenance'},
        'delete_b2_pdfs_task': {'queue': 'maintenance'},
        'delete_book_task': {'queue': 'maintenance'},
        
        # Default queue for miscellaneous tasks
        'celery_app.health_check': {'queue': 'default'},
    },
    
    # Performance optimizations
    task_default_queue="default",
    worker_prefetch_multiplier=1,  # Fetch 1 task at a time (prevents hoarding)
    task_acks_late=True,  # Acknowledge task only after completion
    task_reject_on_worker_lost=True,  # Requeue tasks if worker dies
    broker_connection_retry_on_startup=True,
    
    # Result backend settings
    result_expires=3600,  # Results expire after 1 hour
    result_backend_transport_options={
        'master_name': 'mymaster',
    },
)

@celery_app.task(name="celery_app.health_check")
def health_check():
    return "✅ Celery is alive!"