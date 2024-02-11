import requests
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_active_ingredients_etl():
    active_ingredients = ["acetaminophen",
                      "ibuprofen",
                      "fexofenadine",
                      "loratadine",
                      "hydrocortisone",
                      "dextromethorphan",
                      "pseudoephedrine",
                      "bismuth subsalicylate",
                      "diphenhydramine",
                      "pseudoephedrine"]
    url = "https://api.fda.gov/drug/drugsfda.json"
    product_list = []

    for ingredient_name in active_ingredients:

        querystring = {"search":'products.active_ingredients.name:{}'.format(ingredient_name),"limit":1000}

        results = requests.get(url, params=querystring).json()['results']

        ###################     EXTRACT     ###################

        for result in results:
            for product in result["products"]: # each product

                strength = ""
                other_active_ingredients=""

                for ingredient in product['active_ingredients']: # extract strength and other ingredients
                    if ingredient_name in ingredient['name'].lower():
                        if 'strength' in ingredient.keys():
                            strength = ingredient['strength']
                    else:
                        other_active_ingredients+=ingredient['name']+","
                other_active_ingredients = other_active_ingredients[:len(other_active_ingredients)-1]
                # data for each product
                refined_product = {"id":len(product_list),
                                "active_ingredient": ingredient_name,
                                "application_number": result['application_number'],
                                "sponsor_name": result['sponsor_name'],
                                "brand_name": product['brand_name'],
                                "strength": strength,
                                "other_active_ingredients": other_active_ingredients,
                                "dosage_form": product['dosage_form'],
                                "route": product['route'],
                                "marketing_status": product['marketing_status']}

                product_list.append(refined_product)

    ###################     TRANSFORM     ###################

    df = pd.DataFrame(product_list)

    df = df.replace("","None")

    ###################     LOAD     ###################

    df.to_csv("s3://sl-active-ingredients-bucket/active_ingredients_data.csv")


