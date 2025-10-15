from . import shell_cmd

def gen_docker_compose(clients=1, filters_year=1, filters_hour=1, filters_amount=1, group_by_year_month=1, group_by_semester=1, join_items=1, join_store=1, topk=1):
    code = shell_cmd.silent(f'bash gen.sh docker-compose-dev.yaml {clients} {filters_year} {filters_hour} {filters_amount} {group_by_year_month} {group_by_semester} {join_items} {join_store} {topk}')
    if code != 0:
        raise Exception("Error generating docker-compose file")