CONFIG = """
{
    "new_cluster": {
        "spark_version": "7.6.x-scala2.12",
        "node_type_id": "c4.4xlarge",
        "driver_node_type_id": "r4.4xlarge",
        "autoscale": {
            "min_workers": 2,
            "max_workers": 32
        }
    },
    "libraries": [
        {
            "maven": {
                "coordinates": "com.amazon.deequ:deequ:1.2.2-spark-3.0"
            }
        },
        {
            "maven": {
                "coordinates": "com.github.scopt:scopt_2.12:4.0.1"
            }
        },
        {
            "jar": "dbfs:/FileStore/jars/dna-profiler_2.12-0.4.jar"
        }
    ],
    "spark_jar_task": {
        "main_class_name": "com.katzp.data.profiler.Pipeline"
    }
}"""
