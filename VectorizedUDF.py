# The Snowpark package is required for Python Worksheets.
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
import pandas as pd
from snowflake.snowpark.functions import col


def main(session: snowpark.Session):
    # Your code goes here, inside the "main" handler.
    # Define a general-purpose vectorized UDF
    def udf_blueprint_general(df: pd.DataFrame) -> pd.Series:
        # Import necessary libraries (add any specific libraries needed for the operation)

        # Example transformation: Add a constant value to each element in a specified column
        # Replace this with the desired transformation logic

        # Assuming the first column is the target column for transformation
        target_column = df.iloc[:, 0].astype("float")

        # Perform the transformation (example: add 10 to each element)
        transformed_column = target_column + 10

        # Create a result DataFrame (if needed)
        result_df = pd.DataFrame({"TransformedColumn": transformed_column})

        # Convert the result DataFrame to JSON (if needed)
        result_json = result_df.assign(
            JSON_Column=result_df.agg(pd.Series.to_dict, axis=1)
        )

        # Return the transformed data as a Pandas Series
        return pd.Series(result_json["JSON_Column"])

    # Example usage (within Snowflake)
    # Assuming you have the necessary imports and Snowflake connection setup
    import snowflake.connector
    import snowflake.snowpark
    from snowflake.snowpark import Session
    from snowflake.snowpark.functions import udf

    # Define Snowflake session (fill in with your Snowflake account details)
    # session = Session.builder.configs(connection_parameters).create()

    # Register the UDF in Snowflake
    import snowflake.snowpark.types as T

    vectorized_udf = session.udf.register(
        func=udf_blueprint_general,
        input_types=[T.FloatType()],
        return_type=T.VariantType(),
        name="se_demo_db.snowpark_udf.udf_blueprint_general",
        is_permanent=False,
        replace=True,
    )
