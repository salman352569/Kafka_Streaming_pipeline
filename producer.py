from datetime import datetime
from faker import Faker
import random
import time
from confluent_kafka import Producer
import json
import uuid
import signal
import logging
import os
import signal

logging.basicConfig(
    level =logging.INFO,
    format="%(asctime)s  [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log=logging.getLogger(__name__)

# ________________________________________________________________________________________________
# CONFIG (sINGLE PLACE TO CHNAGE SETTINGS - NO MORE HARD CODED VALUES)
#_________________________________________________________________________________________________

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC","test_Topic")
TOTAL_MESSAGES          =int(os.getenv("TOTAL_MESSAGES", "1000"))
BATCH_SIZE              =int(os.getenv("BATCH_SIZE","200"))
MESSAGE_DELAY_SECOND    =float(os.getenv("MESSAGE_DELAY_SECOND","0.01"))
# ─────────────────────────────────────────────────────────────
#  DIAGNOSIS MAP
# ─────────────────────────────────────────────────────────────

diagnosis_map={
    "E11.9": "Type 2 diabetes mellitus without complications",
    "I10": "Essential (primary) hypertension",
    "J45.9": "Asthma, unspecified",
    "J18.9":"pneumonia , unspecified organism",
    "I21.9":"Acute myocardial infarction,unspecified",
    "N18.3":"Chronic kidney disease, stage 3",
    "F32.9": "Major depressive disorder, single episode, unspecified",
    "M54.5": "Low back pain",
    "K29.7": "Gastritis, unspecified",
    "Z87.891": "Personal history of nicotine dependence",
}

fake =Faker()
# Global flag for graceful shutdown
running =True
def _signal_handler(sig,frame):
     global running
     log.warning("Shutdown signal received - finishing current batch then stopping.")
     running = False

signal.signal(signal.SIGINT,_signal_handler)
signal.signal(signal.SIGTERM,_signal_handler)

# Delivery report callback
def delivery_report(err,msg):
    if err is not None:
        log.error(f"Delivery Falied: %s",err)
    
# ─────────────────────────────────────────────────────────────
#  RECORD GENERATOR
# ─────────────────────────────────────────────────────────────
def generate_patient_record() -> str:
    dob               = fake.date_of_birth(minimum_age=20,maximum_age=80)
    diagnosis_code    = random.choice(list(diagnosis_map.keys()))
    age               = int((datetime.now().date() - dob).days /365)

    record = {
        "id":             str(uuid.uuid4()),          # Unique ID for every patient visit
        "timestamp":      datetime.now().isoformat(),  # When this record was created
        "first_name":     fake.first_name(),
        "last_name":      fake.last_name(),
        "dob":            dob.strftime("%Y-%m-%d"),
        "age":            age,
        "gender":         random.choice(["M", "F"]),
        "phone":          fake.phone_number(),
        "address":        fake.address().replace("\n", ", "),
        "doctor_id":      random.randint(1000, 9999),
        "doctor_name":    f"Dr. {fake.last_name()}",  # FIX: was just fake.last_name()
        "ward":           random.randint(1, 20),
        "room":           random.randint(1, 30),
        "wbc":            round(random.uniform(3.5, 12.0), 1),
        "hgb":            round(random.uniform(11.0, 16.0), 1),
        "allergy":        random.choice(["Penicillin", "Latex", "Peanuts", "None"]),
        "severity":       random.choice(["Mild", "Moderate", "Severe"]),
        "diagnosis_code": diagnosis_code,
        "diagnosis_desc": diagnosis_map[diagnosis_code],
    }
    return json.dumps(record)


# Kafka Producer Simulation.
def simulate_kafka_stream():
    
       kafka_conf ={
           "bootstrap.servers":        KAFKA_BOOTSTRAP_SERVERS,
           "linger.ms":                100,
           "queue.buffering.max.messages":100_00,
           "batch.num.messages":       BATCH_SIZE,
       }

       producer=Producer(kafka_conf)
       message_count =0
       failed_count = 0
       start_time =time.time()

       log.info("=" * 60)
       log.info("Kafka producer started")
       log.info("Topic : %s",KAFKA_TOPIC)
       log.info("Target: %d messages in batches of %d",TOTAL_MESSAGES,BATCH_SIZE)
       log.info("=" * 60)

       while running and message_count < TOTAL_MESSAGES:
          try:
              message= generate_patient_record()

              producer.produce(
                  KAFKA_TOPIC,
                  value=message.encode("utf-8"),
                  callback=delivery_report,
              )
              producer.poll(0)
              message_count += 1

              if message_count % 100 == 0:
                  log.info("progress: %d /%d message sent ",message_count,TOTAL_MESSAGES)

              time.sleep(MESSAGE_DELAY_SECOND)
          except BufferError:
              log.warning("Kafka buffer full -waiting 1 second for drain .....")
              producer.poll(1)
          except Exception as exc:
              log.error("Enexcepted error generating /sending record :%s",exc)
              failed_count += 1
        
       log.info("Flushing remaining messages .....")
       producer.flush()

       elapsed =time.time() - start_time
       log.info("=" * 60)
       log.info("PRODUCER SUMMARY")
       log.info(" Sent   : %d",message_count)
       log.info("  Failed  :%d",failed_count)
       log.info("   Time  :%.2f seconds", elapsed)
       log.info("   Rate: :%.2f msg/sec", message_count / elapsed if elapsed > 0 else 0)


if __name__ == "__main__":
    simulate_kafka_stream()