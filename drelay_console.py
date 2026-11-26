#!/usr/bin/env python3 -I
"""值班端启动脚本：在隔离模式下把仓库根加入 sys.path 后转交 CLI。

使用 -I 运行，避免当前目录或环境变量里的可疑模块（如伪造的 json.py）
被解释器加载；路径只来自这个受控脚本。
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from drelay.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
