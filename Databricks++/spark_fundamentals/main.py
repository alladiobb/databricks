import pyspark.pandas as pd

pd.set_option("compute.default_index_type", "distributed-sequence")

#pyarrow
#vale a pena testar abaixo as opções
#pd.set_option("compute.default_index_type", "distributed")

#pd.set_option("compute.default_index_type", "sequence")

retail = "dfbs:/mnt/retail/retail_reviews.csv"

pd_previews = pd.read_json(retail, lines=True)
pd_previews.olic[:10]

retail = pd.read_json(retail, lines=True, index_col="review_id")

