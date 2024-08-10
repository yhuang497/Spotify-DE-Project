import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Albums
Albums_node1723286273131 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-de-project-yc/staging/albums.csv"], "recurse": True}, transformation_ctx="Albums_node1723286273131")

# Script generated for node Artists
Artists_node1723286274084 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-de-project-yc/staging/artists.csv"], "recurse": True}, transformation_ctx="Artists_node1723286274084")

# Script generated for node Track
Track_node1723286274584 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-de-project-yc/staging/track.csv"], "recurse": True}, transformation_ctx="Track_node1723286274584")

# Script generated for node Join with Albums & Artists
JoinwithAlbumsArtists_node1723286376138 = Join.apply(frame1=Albums_node1723286273131, frame2=Artists_node1723286274084, keys1=["artist_id"], keys2=["id"], transformation_ctx="JoinwithAlbumsArtists_node1723286376138")

# Script generated for node Join with Track
JoinwithTrack_node1723286448530 = Join.apply(frame1=JoinwithAlbumsArtists_node1723286376138, frame2=Track_node1723286274584, keys1=["track_id"], keys2=["track_id"], transformation_ctx="JoinwithTrack_node1723286448530")

# Script generated for node Drop Fields
DropFields_node1723286597228 = DropFields.apply(frame=JoinwithTrack_node1723286448530, paths=["id", "`.track_id`"], transformation_ctx="DropFields_node1723286597228")

# Script generated for node Destination
Destination_node1723286632410 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1723286597228, connection_type="s3", format="glueparquet", connection_options={"path": "s3://spotify-de-project-yc/data warehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1723286632410")

job.commit()