import boto3

def handle_create_action_event(event):
    print("Handling Create Workflow action")


def ngs360_event_handler(event):
    print(f"Received event: {event}")
    if event.get('action') == 'Create Workflow':
        handle_create_action_event(event)