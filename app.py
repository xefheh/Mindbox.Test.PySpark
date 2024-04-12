from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

def get_products_and_categories_pairs(products_df: DataFrame, categories_df: DataFrame) -> DataFrame:
    '''Получить пары продуктов и категорий, с учётом продуктов без категорий'''
    joined_df = products_df \
        .join(categories_df, 'category_id', how='left') \
        .select('product_name', 'category_name') \
        .orderBy(col('category_name').isNull().asc())
    return joined_df
    

if __name__== '__main__' :
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Default name") \
        .getOrCreate()
    
    categories_df = spark.createDataFrame([(1, 'Высшая'),
                                       (2, 'Такое себе'),
                                       (3, 'Нормальная'),
                                       (4, 'Что это')],
                                      ['category_id', 'category_name']);
    
    products_df = spark.createDataFrame([(1, 'Банан', 1),
                                      (2, 'Гидрант', 2),
                                      (3, 'Хлеб', None),
                                      (4, 'Кто', None),
                                      (5, 'Восьмёрка', 4)],
                                    ['product_id', 'product_name', 'category_id']);
    
    get_products_and_categories_pairs(products_df, categories_df) \
        .show()
