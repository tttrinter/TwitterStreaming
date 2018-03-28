import boto3

def notify(message):
    boto = boto3.setup_default_session(profile_name='di')
    client = boto3.client('sns',
    region_name="us-east-1")

    #arn:aws:sns:us-east-1:178603499269:TwitterStream

    # Send your sms message.
    try:
      r =  client.publish(
            PhoneNumber="+12067553147",
            Message=message
        )
      #print(r)
      return r
    except Exception as e:
        print(e)
        return e

    # Send your sms message.
    client.publish(
        PhoneNumber="+13023771148",
        Message=message
    )