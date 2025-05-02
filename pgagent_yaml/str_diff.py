import difflib


# https://gist.github.com/KaoruNishikawa/55781991af9d50494dd85ac6db0cad89
def color_str_diff(a: str, b: str) -> str:
    line_color = {"+": 32, "-": 31}

    diffs = difflib.ndiff(a.splitlines(keepends=True), b.splitlines(keepends=True))
    diff_list = list(diffs)
    styled: list[str] = []
    for prev, next in zip(diff_list, diff_list[1:] + [""]):
        color = line_color.get(prev[0], 0)
        if prev[0] == " ":
            styled.append(prev)
        if prev[0] in ("+", "-"):
            index = [i for i, c in enumerate(next) if c == "^"]
            _prev = list(prev)
            for idx in index:
                _prev[idx] = f"\x1b[97;{color+10};1m{_prev[idx]}\x1b[0;{color}m"
            styled.append(f'\x1b[{color}m{"".join(_prev)}\x1b[0m')
        if prev[0] == "?":
            continue
    return "".join(styled)
