# Course: it5427 - Big Data #
# Project Topic: Multi-Source Big Data for Credit Risk Assessment and Prediction (Credit Risk Intelligence) #
# Datatset: Home Credit Default Risk (_data/) #

# Project overview # 
The team’s primary objective is to integrate and process big data from various sources to assess and forecast credit risk. Specifically, the team plans to build a pipeline `collect → integrate → store → process → analyze → predict`, using publicly available Home Credit Default Risk data as the primary source and potentially supplementing with simulated/real data to illustrate multi-source integration. Technically, the team intends to use Python and Kafka for data collection, MinIO as a data lake, and Spark for data cleaning and integration. This will be followed by the development of a model to predict the probability of default, alongside a dashboard to support decision-making. The project scope is structured in three tiers: an MVP (Home Credit dataset + Spark); an intermediate stage incorporating MinIO and Kafka for batch processing and simulated streaming; and an extended phase that integrates external data and Explainable AI/ML techniques.

# Assigning tasks within the team #
## Member 1 - Data Collection & Storage ##
- Write Python code to ingest data into the Data Lake.
- Set up MinIO and design the Data Lake structure, consisting of Raw, Clean, and Processed layers.
- Validate data formats, file sizes, data fields, and check for missing values.
- Identify or generate supplementary data, such as occupation, overall economic growth of the region, educational attainment, etc.
- Examine the following tables: `application`, `previous_application`, `installments`, `bureau`, etc.
- Locate and download the "Home Credit Default Risk" dataset from Kaggle.

## Member 2 - Data Integration & Processing ##
- In-depth study of Delta Lake: architecture, functionality, advantages, and integration with Spark.
- Save processed data into the 'Processed' layer.
- Engineer features such as the number of late payments, total outstanding balance, loan-to-income ratio, etc.
- Utilize Apache Spark for data integration and processing.
- Perform data cleaning: handle missing values, duplicates, and formatting errors.
- Identify the keys for linking tables.
- Examine the structure of the data tables.

## Our task - Analytics & Machine Learning ##
- Explain the factors influencing predictions (e.g., using SHAP).
- Segment customers into low, medium, and high-risk categories.
- Evaluate the model using Precision, Recall, F1-score, and ROC-AUC.
- Train a model to predict the likelihood of default (using Logistic Regression, Random Forest, or XGBoost).
- Analyze risk based on factors such as income, occupation, and credit history.
- Calculate the non-performing loan (NPL) ratio, delinquency rate, and average loan amount.
- Perform statistical analysis of the data following integration.

## Member 3 - Dashboard, Integration & Deployment Validate ##
- Perform end-to-end system testing.
- Prepare installation guides and demo walkthroughs.
- Integrate individual modules into a complete, unified system.
- Connect the dashboard to processed data and machine learning (ML) outputs.
- Display forecasting results and the factors influencing them.
- Develop visualizations, including non-performing loan (NPL) ratios, risk distribution, and risk analysis by income, occupation, etc.
- Design the dashboard interface.

`Note: We only need to handle the tasks that in the scope of "Our task." Other work items will be taken on by other team members and carried out in parallel.`