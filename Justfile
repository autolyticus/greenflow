ntfy_url := env_var_or_default('NTFY_URL', "https://ntfy.sh/your_url_here")
gitroot := env_var_or_default('GITROOT', "./")
set positional-arguments

send_notification message:
    #!/usr/bin/env python
    import requests
    requests.post("{{ntfy_url}}", headers={"priority": "low"}, data="{{message}}")

test_message_delivery:
    just send_notification "Test message"

setup *args:
    python entrypoint.py setup "$@"

test *args:
    python entrypoint.py test "$@"

ingest *args:
    python entrypoint.py ingest "$@"

i *args:
    python entrypoint.py i "$@"

killjob:
    python entrypoint.py killjob

screen:
    #!/usr/bin/env python
    from greenflow.screen import ExperimentApp
    
    TA = ExperimentApp()
    TA.run()
