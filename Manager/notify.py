import boto3

def notify(message):
    boto = boto3.setup_default_session(profile_name='di')
    client = boto3.client('sns',
    region_name="us-east-1")

    # Send your sms message.
    client.publish(
        PhoneNumber="+12067553147",
        Message=message
    )

    # Send your sms message.
    client.publish(
        PhoneNumber="+13023771148",
        Message=message
    )