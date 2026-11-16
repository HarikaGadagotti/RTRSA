
# Real-Time Restaurant Reviews Sentiment Analysis

A real-time data streaming project that processes restaurant customer reviews using Apache Kafka and Apache Spark Streaming, performs AI-based sentiment classification using GPT through LangChain, and stores the processed results in Elasticsearch for analysis and visualization.


## Key Features

- **Real-Time Data Streaming:** Uses Apache Kafka to publish restaurant reviews continuously.
- **Stream Processing:** Uses Apache Spark Structured Streaming to consume and process Kafka messages.
- **AI-Based Sentiment Analysis:** Uses GPT through LangChain to classify customer reviews.
- **Data Storage:** Stores processed reviews and sentiment results in Elasticsearch.
- **Visualization:** Elasticsearch data can be explored and visualized using Kibana.
- **Simulated Data Generation:** Generates sample restaurant reviews with customer and restaurant information.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Application development |
| Apache Kafka | Real-time message streaming |
| Apache Spark Streaming | Stream processing |
| LangChain | Integration with the GPT model |
| OpenAI GPT | Sentiment classification |
| Elasticsearch | Processed review storage |
| Kibana | Data exploration and visualization |

## System Architecture

```text
+------------+     +------------------------+     +----------------+     +------------------+     +---------------+     +----------+
|            |     |      Apache Kafka      |     |                |     |                  |     |               |     |          |
|            |     | +--------------------+ |     |  Apache Spark  |     |    LangChain     |     |               |     |          |
|  Producer  |     | |       Topic:       | |     |   Structured   |     |        +         |     | Elasticsearch |     |  Kibana  |
| Simulated  | --> | | restaurant_reviews | | --> |   Streaming    | --> |   OpenAI GPT     | --> |  Stores data  | --> |Dashboards|
|  reviews   |     | +--------------------+ |     |                |     |                  |     |               |     |          |
|            |     |                        |     |  JSON parsing  |     |    Sentiment     |     |               |     |          |
|            |     |                        |     |                |     |  classification  |     |               |     |          |
+------------+     +------------------------+     +----------------+     +------------------+     +---------------+     +----------+
```

## Project Workflow

### 1. Review Generation

The Kafka producer generates sample restaurant reviews containing:

- review_id
- restaurant_id
- review_text
- rating
- timestamp

The producer continuously sends review messages to the Kafka topic named `restaurant_reviews`.

### 2. Kafka Message Streaming

Apache Kafka acts as the message broker and transfers incoming restaurant reviews from the producer to the consumer.

### 3. Stream Processing with Apache Spark

Spark Structured Streaming connects to the Kafka topic and continuously reads incoming messages.

The incoming JSON messages are parsed using a predefined schema to extract review details.

### 4. Sentiment Classification

The extracted review text is passed to a GPT model through LangChain.

The model analyzes the review and returns a sentiment classification.

### 5. Elasticsearch Storage

The processed review, along with its sentiment result, is indexed in Elasticsearch under the index:

`restaurant_reviews`

### 6. Visualization and Analysis

Kibana can be connected to Elasticsearch to explore the stored review data and create visualizations based on sentiment and restaurant information.

## Repository Structure

```text
RTRSA/
│
├── kafkaproducer1.py
│   └── Generates and publishes simulated restaurant reviews
│
├── finalconsumer.py
│   └── Consumes Kafka messages, processes reviews,
│       performs sentiment classification, and stores results
│
├── reviews_dataset.csv
│   └── Restaurant reviews dataset
│
└── README.md
```

## Prerequisites

Install and configure the following before running the project:

- Python
- Apache Kafka
- Apache Spark
- Java Development Kit
- Elasticsearch
- Kibana (optional, for visualization)
- OpenAI API key

## Installation and Setup

### 1. Clone the Repository

```bash
git clone https://github.com/HarikaGadagotti/RTRSA.git
cd RTRSA
```

### 2. Install Python Dependencies

```bash
pip install kafka-python pyspark datasketch elasticsearch langchain openai
```

Note: The project uses LangChain's older `ChatOpenAI` import. Depending on the installed LangChain version, you may need compatible package versions or update the import.

### 3. Configure OpenAI API Key

Set your API key as an environment variable rather than committing it to the source code.

**Linux / WSL:**

```bash
export OPENAI_API_KEY="your_api_key"
```

**Windows PowerShell:**

```powershell
$env:OPENAI_API_KEY="your_api_key"
```

Ensure the consumer reads the key from the environment variable before starting.

### 4. Start Apache Kafka

Start your Kafka broker and create the topic:

```bash
kafka-topics.sh --create \
  --topic restaurant_reviews \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

The Kafka broker must be accessible at `localhost:9092`, as configured in the project code.

### 5. Start Elasticsearch

Start Elasticsearch and ensure it is accessible at:

```text
http://localhost:9200
```

Kibana can be started separately if visualization is required.

### 6. Run the Kafka Producer

Open a terminal and execute:

```bash
python kafkaproducer1.py
```

The producer continuously generates restaurant reviews and publishes them to Kafka.

### 7. Run the Spark Streaming Consumer

Open another terminal and execute:

```bash
python finalconsumer.py
```

The consumer reads messages from Kafka, performs sentiment classification, and stores the processed records in Elasticsearch.

**Important:** Spark must have the matching Kafka connector available when running Structured Streaming. Configure the appropriate Spark Kafka package for your installed Spark version before starting the consumer.

## Example Review

**Input:**

```json
{
  "order_id": 1205,
  "restaurant_name": "Pizza Hub",
  "customer_name": "John Doe",
  "review": "Amazing food! Liked it.",
  "timestamp": "2025-04-15 12:30:00"  
}
```

**Illustrative processed result:**
**Input:**

```json
{
  "review_id": 1205,
  "restaurant_id": 301,
  "review_text": "Amazing food! Loved it.",
  "rating": 5,
  "timestamp": "2025-04-15 12:30:00"
}
```

**Illustrative processed result:**

```json
{
  "review_id": 1205,
  "restaurant_id": 301,
  "review_text": "Amazing food! Liked it.",
  "rating": 5,
  "timestamp": "2025-04-15 12:30:00",
  "sentiment": "positive"
}
```

## Future Enhancements

- Integrate a larger real-world restaurant review dataset.
- Add sentiment distribution dashboards in Kibana.
- Improve error handling and retry mechanisms.
- Introduce additional sentiment categories and analytics.
- Optimize streaming performance for larger data volumes.
- Add automated testing and monitoring for the pipeline.



