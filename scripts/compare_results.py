import sys


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


def _compare_sets_with_tolerance(name, current_set, expected_set, tolerance=0.01, sample_size=3):
    """
    Compara dos conjuntos de resultados con tolerancia en valores flotantes.
    """
    matches = 0
    only_in_current = set()
    only_in_expected = set()

    # Convert expected_set to a list for easier searching
    expected_list = list(expected_set)
    
    for current_item in current_set:
        found_match = False
        for expected_item in expected_list:
            if _matches_with_tolerance(current_item, expected_item, tolerance):
                matches += 1
                expected_list.remove(expected_item)
                found_match = True
                break
        
        if not found_match:
            only_in_current.add(current_item)
    
    # Remaining items in expected_list are only in expected
    only_in_expected = set(expected_list)
    
    total_items = len(current_set)
    if matches == total_items and len(only_in_expected) == 0:
        print(f"✅ {name}: Todos los resultados coinciden ({total_items} filas).")
        return

    print(f"❌ {name}: Los resultados difieren.")
    print(f" - Filas coincidentes: {matches}")
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


def _matches_with_tolerance(current_item, expected_item, tolerance):
    """
    Compara dos strings con tolerancia para valores flotantes.
    """
    if current_item == expected_item:
        return True
    
    # Split by comma and check if we have exactly 3 parts (semester, store, total)
    current_parts = current_item.split(",")
    expected_parts = expected_item.split(",")
    
    if len(current_parts) != 3 or len(expected_parts) != 3:
        return False
    
    # Check if semester and store match exactly
    if current_parts[0] != expected_parts[0] or current_parts[1] != expected_parts[1]:
        return False
    
    # Compare the float values with tolerance
    try:
        current_float = float(current_parts[2])
        expected_float = float(expected_parts[2])
        return abs(current_float - expected_float) <= tolerance
    except ValueError:
        return False


def compare_results_q1(client_id, expected_folder="expected_results"):
    name = "results_q1"
    current_results = f"./results/results_q1_{client_id}.txt"
    
    if expected_folder == "expected_results_full":
        expected_results = f"./scripts/expected_results_full/results_q1.txt"
        lines_expected_results = set()
        lines_current_results = set()

        with open(current_results, "r", encoding="utf-8") as f:
            for line in f:
                id, amount = line.strip().split(",")
                formatted = f"{id},{float(amount):.1f}"
                lines_current_results.add(formatted)

        with open(expected_results, "r", encoding="utf-8") as f:
            for line in f:
                id, amount = line.strip().split(",")
                formatted = f"{id},{float(amount):.1f}"
                lines_expected_results.add(formatted)
    else:
        expected_results = "./scripts/expected_results/results_q1.csv"
        lines_current_results = set()
        lines_expected_results = set()

        with open(current_results, "r", encoding="utf-8") as f:
            for line in f:
                id, amount = line.strip().split(",")
                formatted = f"{id},{float(amount):.1f}"
                lines_current_results.add(formatted)

        with open(expected_results, "r", encoding="utf-8") as f:
            next(f)
            for line in f:
                id, amount = line.strip().split(",")
                formatted = f"{id},{float(amount):.1f}"
                lines_expected_results.add(formatted)

    _compare_sets(name, lines_current_results, lines_expected_results)


def compare_results_q2(client_id, expected_folder="expected_results"):
    name = "results_q2_best_sellers"
    current_results_path = f"./results/results_q2_{client_id}.txt"
    
    if expected_folder == "expected_results_full":
        expected_results_path = f"./scripts/expected_results_full/results_q2.txt"

        current_results = set()
        expected_results = set()

        with open(current_results_path, "r") as file:
            for line in file:
                current_results.add(line.strip())

        with open(expected_results_path, "r") as file:
            for line in file:
                expected_results.add(line.strip())
    else:
        expected_results_path = "./scripts/expected_results/results_q2.csv"

        current_results = set()
        expected_results = set()

        with open(current_results_path, "r") as file:
            for line in file:
                current_results.add(line.strip())

        with open(expected_results_path, "r") as file:
            for line in file:
                expected_results.add(line.strip())

    _compare_sets(name, current_results, expected_results)


def compare_results_q3(client_id, expected_folder="expected_results"):
    name = "results_q3"
    current_results_path = f"./results/results_q3_{client_id}.txt"
    
    def parse_line(line: str) -> str:
        semester, store, total = line.strip().split(",")
        return f"{semester},{store},{float(total):.2f}"

    if expected_folder == "expected_results_full":
        expected_results_path = f"./scripts/expected_results_full/results_q3.txt"

        current_results = set()
        expected_results = set()

        with open(current_results_path, "r") as file:
            for line in file:
                current_results.add(parse_line(line))

        with open(expected_results_path, "r") as file:
            for line in file:
                expected_results.add(parse_line(line))
    else:
        expected_results_path = "./scripts/expected_results/results_q3.csv"

        current_results = set()
        expected_results = set()

        with open(current_results_path, "r") as file:
            for line in file:
                current_results.add(parse_line(line))

        with open(expected_results_path, "r") as file:
            next(file)
            for line in file:
                expected_results.add(parse_line(line))

    # Use tolerance comparison for Q3 due to potential floating point precision differences
    _compare_sets_with_tolerance(name, current_results, expected_results, tolerance=0.1)


def compare_results_q4(client_id, expected_folder="expected_results"):
    name = "results_q4"
    current_results_path = f"./results/results_q4_{client_id}.txt"
    
    def parse_line(line: str) -> str:
        parts = line.strip().split(",")
        store = parts[0]
        bday = parts[1]
        return f"{store},{bday}"

    if expected_folder == "expected_results_full":
        expected_results_path = f"./scripts/expected_results_full/results_q4.txt"

        current_results = set()
        expected_results = set()

        with open(current_results_path, "r") as file:
            for line in file:
                current_results.add(parse_line(line))

        with open(expected_results_path, "r") as file:
            for line in file:
                expected_results.add(parse_line(line))
    else:
        expected_results_path = "./scripts/expected_results/results_q4.csv"

        current_results = set()
        expected_results = set()

        with open(current_results_path, "r") as file:
            for line in file:
                current_results.add(parse_line(line))

        with open(expected_results_path, "r") as file:
            next(file)
            for line in file:
                expected_results.add(parse_line(line))

    _compare_sets(name, current_results, expected_results)


if __name__ == "__main__":
    client_id = sys.argv[1] if len(sys.argv) > 1 else 1
    expected_folder = sys.argv[2] if len(sys.argv) > 2 else "expected_results"
    
    print(f"Comparando resultados para client_id {client_id}")
    print(f"Usando carpeta esperada: {expected_folder}")
    
    try:
        compare_results_q1(client_id, expected_folder)
    except Exception as e:
        print(f"Error found: [{e.__class__.__name__}] {e}")
    try:
        compare_results_q2(client_id, expected_folder)
    except Exception as e:
        print(f"Error found: [{e.__class__.__name__}] {e}")
    try:
        compare_results_q3(client_id, expected_folder)
    except Exception as e:
        print(f"Error found: [{e.__class__.__name__}] {e}")
    try:
        compare_results_q4(client_id, expected_folder)
    except Exception as e:
        print(f"Error found: [{e.__class__.__name__}] {e}")
