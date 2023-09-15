# spotify-end-to-end-aws-snowflake

**Objective:**

Seamlessly integrated Spotify API to extract and analyze music data. 🎶 Leveraging Python, Amazon Web Services (AWS), and Snowflake, by building an end-to-end data pipeline to process, transform, and visualize Spotify's rich music data.
The primary objective was to extract valuable insights from Spotify's extensive music catalog while constructing a comprehensive data pipeline to automate the entire process.


**🐍 Programming Language - Python:**
Python served as the cornerstone, enabling seamless interaction with Spotify's API and handling data effectively.


**☁️ Amazon Web Services (AWS):**
I harnessed the power of AWS, deploying AWS Lambda functions for data extraction, transformation, and loading. Amazon S3 played a crucial role in storing both raw and transformed data efficiently.


**🌨️ Snowflake for Data Storage:**
For the loading part, I employed Snowflake, an ideal platform for data warehousing and analytics. Snowpipe, a component of Snowflake, automatically ingested and updated the data, ensuring real-time availability.


**📊 Power BI for Visualization:**
Can utilise Power BI to craft stunning data visualizations and interactive dashboards, simplifying data-driven decision-making.

**Architecture:**

<img width="991" alt="image" src="https://github.com/imkaran45/spotify-end-to-end-aws-snowflake/assets/28987765/5cb82f31-3be7-4e6a-bd02-cf361c910ac9">



**🔍 Project Components:**

**Data Extraction:** Python code, aided by the Spotipy library, fetched music data directly from Spotify, and subsequently, it was stored in AWS S3.

**Automated Data Transformation:** I designed AWS Lambda functions to efficiently convert JSON data into structured tables, encompassing songs, albums, and artists.

**AWS Lambda Functions:** These serverless functions automated the entire data processing pipeline whenever new data became available.

**Amazon S3 & Snowflake Integration:** Transformed data was stored in AWS S3 and seamlessly loaded into Snowflake, creating analytics-ready tables.
