def _compare_sets(name, current_set, expected_set, sample_size=3):
    """
    Compara dos conjuntos de resultados e imprime un resumen con ejemplos de diferencias.
    """
    if current_set == expected_set:
        print(f"✅ {name}: Todos los resultados coinciden ({len(current_set)} filas).")
        return

    only_in_current = current_set - expected_set
    only_in_expected = expected_set - current_set

    print(f"❌ {name}: Los resultados difieren.")
    print(f" - Líneas únicas en resultados actuales: {len(only_in_current)}")
    print(f" - Líneas únicas en resultados esperados: {len(only_in_expected)}")

    # Mostrar ejemplos
    if only_in_current:
        print("\nEjemplos solo en resultados actuales:")
        for line in list(sorted(only_in_current))[:sample_size]:
            print(f"   {line}")
    if only_in_expected:
        print("\nEjemplos solo en resultados esperados:")
        for line in list(sorted(only_in_expected))[:sample_size]:
            print(f"   {line}")
    print()  # línea vacía para separación visual


def compare_results_q1(client_id):
    name = "results_q1"
    current_results = "./results/results_q1_2.txt"
    expected_results = "./scripts/expected_results/results_q1.csv"

    lines_current_results = set()
    lines_expected_results = set()

    with open(current_results, "r", encoding="utf-8") as f:
        for line in f:
            id, store, _, amount, date = line.strip().split(',')
            formatted = f"{id},{float(amount):.1f}"
            lines_current_results.add(formatted)
    
    with open(expected_results, "r", encoding="utf-8") as f:
        next(f)
        for line in f:
            id, amount = line.strip().split(',')
            formatted = f"{id},{float(amount):.1f}"
            lines_expected_results.add(formatted)

    _compare_sets(name, lines_current_results, lines_expected_results)


def compare_results_q2_top_earners(client_id):
    name = "results_q2_top_earners"
    current_results_path = f"./results/results_q2_{client_id}.txt"
    expected_results_path = "./scripts/expected_results/results_q2_top_earners.csv"

    current_results = set()
    expected_results = set()
    
    with open(current_results_path, 'r') as file:
        for line in file:
            yearMonth, item, qty, profit = line.strip().split(',')
            if not float(profit) > 0:
                continue
            current_results.add(f"{yearMonth},{item},{float(profit):.2f}")

    with open(expected_results_path, 'r') as file:
        next(file)
        for line in file:
            yearMonth, item, profit = line.strip().split(',')
            expected_results.add(f"{yearMonth},{item},{float(profit):.2f}")

    _compare_sets(name, current_results, expected_results)


def compare_results_q2_best_sellers(client_id):
    name = "results_q2_best_sellers"
    current_results_path = f"./results/results_q2_{client_id}.txt"
    expected_results_path = "./scripts/expected_results/results_q2_best_sellers.csv"

    current_results = set()
    expected_results = set()
    
    with open(current_results_path, 'r') as file:
        for line in file:
            yearMonth, item, qty, profit = line.strip().split(',')
            if not int(qty) > 0:
                continue
            current_results.add(f"{yearMonth},{item},{qty}")

    with open(expected_results_path, 'r') as file:
        next(file)
        for line in file:
            yearMonth, item, qty = line.strip().split(',')
            expected_results.add(f"{yearMonth},{item},{qty}")

    _compare_sets(name, current_results, expected_results)


def compare_results_q3(client_id):
    name = "results_q3"
    current_results_path = f"./results/results_q3_{client_id}.txt"
    expected_results_path = "./scripts/expected_results/results_q3.csv"

    def parse_line(line: str) -> str:
        semester, store, total = line.strip().split(',')
        return f"{semester},{store},{float(total):.2f}"

    current_results = set()
    expected_results = set()

    with open(current_results_path, 'r') as file:
        for line in file:
            current_results.add(parse_line(line))

    with open(expected_results_path, 'r') as file:
        next(file)
        for line in file:
            expected_results.add(parse_line(line))

    _compare_sets(name, current_results, expected_results)


def compare_results_q4(client_id):
    name = "results_q4"
    current_results_path = f"./results/results_q4_{client_id}.txt"
    expected_results_path = "./scripts/expected_results/results_q4.csv"

    def parse_line(line: str) -> str:
        parts = line.strip().split(',')
        store = parts[0]
        bday = parts[1]
        return f"{store},{bday}"

    current_results = set()
    expected_results = set()

    with open(current_results_path, 'r') as file:
        for line in file:
            current_results.add(parse_line(line))

    with open(expected_results_path, 'r') as file:
        next(file)
        for line in file:
            expected_results.add(parse_line(line))

    _compare_sets(name, current_results, expected_results)

if __name__ == "__main__":
    client_id = 2
    compare_results_q1(client_id)
    compare_results_q2_top_earners(client_id)
    compare_results_q2_best_sellers(client_id)
    compare_results_q3(client_id)
    compare_results_q4(client_id)
