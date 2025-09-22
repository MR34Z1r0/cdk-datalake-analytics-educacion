import boto3

client_glue = boto3.client("glue")


def lambda_handler(event, context):

    try:
        crawler_name = event["CRAWLER_NAME"]

        response_crawler = client_glue.get_crawler(Name=crawler_name)
        state = response_crawler["Crawler"]["State"]

        if state == "READY":
            return {"wait": False, "error": False}
        return {"wait": True, "error": False}
    except Exception as e:
        return {"error": True, "msg": f"{str(e)}"}
