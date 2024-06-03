from pyspark.sql import SparkSession  
from pyspark.sql.functions import col  
from pyspark.sql.utils import AnalysisException  
import smtplib  
from email.mime.multipart import MIMEMultipart  
from email.mime.text import MIMEText  
  
# Initialize SparkSession  
spark = SparkSession \  
    .builder \  
    .appName("Ad Data Processing and Storage") \  
    .getOrCreate()  
  
try:  
    # Read a CSV file  
    df = spark.read.csv("/path/to/your/file.csv", inferSchema=True, header=True)  
  
    # Perform some transformations  
    df = df.withColumnRenamed("oldColumnName", "newColumnName")  # Rename a column  
    df = df.filter(col("someColumn") > 0)  # Filter rows where someColumn > 0  
    df.show()  # Print the DataFrame  
  
except AnalysisException as e:  
    # Handle exceptions caused by analysis errors (e.g. non-existent tables or columns)  
    print("AnalysisException: ", e)  
  
    # Send an alert email  
    msg = MIMEMultipart()  
    msg['From'] = 'your-email@example.com'  
    msg['To'] = 'your-email@example.com'  
    msg['Subject'] = 'Spark job failed'  
    body = 'Your Spark job failed with the following error: ' + str(e)  
    msg.attach(MIMEText(body, 'plain'))  
  
    server = smtplib.SMTP('smtp.example.com', 587)  
    server.starttls()  
    server.login('your-email@example.com', 'your-password')  
    text = msg.as_string()  
    server.sendmail('your-email@example.com', 'your-email@example.com', text)  
    server.quit()  
  
except Exception as e:  
    # Handle all other exceptions  
    print("Exception: ", e)  
