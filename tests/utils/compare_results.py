
def _compare_sets(name, current_set, expected_set, sample_size=3):
    """
    Compara dos conjuntos de resultados e imprime un resumen con ejemplos de diferencias.
    """
    if current_set == expected_set:
        print(f"✅ {name}: Todos los resultados coinciden ({len(current_set)} filas).")
        return True

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
    return False


def compare_results_q1(q1_path):
    name = "results_q1"
    expected_results = "./tests/expected_results/results_q1.csv"

    lines_current_results = set()
    lines_expected_results = set()

    with open(q1_path, "r", encoding="utf-8") as f:
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

    return _compare_sets(name, lines_current_results, lines_expected_results)


def compare_results_q2_top_earners(q2_path):
    name = "results_q2_top_earners"
    expected_results_path = "./tests/expected_results/results_q2_top_earners.csv"

    current_results = set()
    expected_results = set()
    
    with open(q2_path, 'r') as file:
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

    return _compare_sets(name, current_results, expected_results)


def compare_results_q2_best_sellers(q2_path):
    name = "results_q2_best_sellers"
    expected_results_path = "./tests/expected_results/results_q2_best_sellers.csv"

    current_results = set()
    expected_results = set()
    
    with open(q2_path, 'r') as file:
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

    return _compare_sets(name, current_results, expected_results)


def compare_results_q3(q3_path):
    name = "results_q3"
    expected_results_path = "./tests/expected_results/results_q3.csv"

    def parse_line(line: str) -> str:
        semester, store, total = line.strip().split(',')
        return f"{semester},{store},{float(total):.2f}"

    current_results = set()
    expected_results = set()

    with open(q3_path, 'r') as file:
        for line in file:
            current_results.add(parse_line(line))

    with open(expected_results_path, 'r') as file:
        next(file)
        for line in file:
            expected_results.add(parse_line(line))

    return _compare_sets(name, current_results, expected_results)


def compare_results_q4(q4_path):
    name = "results_q4"
    expected_results_path = "./tests/expected_results/results_q4.csv"

    current_results = set()
    expected_results = set()

    with open(q4_path, 'r', encoding='utf-8') as file:
        for line in file:
            current_results.add(line.strip('\n'))

    with open(expected_results_path, 'r', encoding='utf-8') as file:
        next(file)
        for line in file:
            expected_results.add(line.strip('\n'))

    return _compare_sets(name, current_results, expected_results)

# if __name__ == "__main__":
#     compare_results_q1()
#     compare_results_q2_top_earners()
#     compare_results_q2_best_sellers()
#     compare_results_q3()
#     compare_results_q4()

def compare_all_results(results: dict[int, list[str]]):
    for client_id in results.keys():
        q1_path, q2_path, q3_path, q4_path = results[client_id]
        resultq1 = compare_results_q1(q1_path)
        resultq2_te = compare_results_q2_top_earners(q2_path)
        resultq2_bs = compare_results_q2_best_sellers(q2_path)
        resultsq3 = compare_results_q3(q3_path)
        resultsq4 = compare_results_q4(q4_path)
        if not (resultq1 and resultq2_te and resultq2_bs and resultsq3 and resultsq4):
            print(f"❌ Resultados del cliente {client_id} no coinciden.")
            return False
    return True