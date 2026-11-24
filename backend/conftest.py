"""pytest 全局夹具：每个用例前清空检查点恢复协调器/模拟湖表的进程内状态，
避免确定性幂等令牌在共享 id 的用例之间发生跨测试污染。

只重置恢复链路自己的单例状态，不重置 app.store 的示例数据。
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _reset_recovery_runtime() -> None:
    try:
        from app.recovery_chain import reset_runtime_state

        # 只清空恢复链路自己的进程内单例（状态板 + 模拟湖表），
        # 不重置 app.store，避免影响其它夹具/用例登记的数据。
        reset_runtime_state()
    except Exception:
        # 应用尚不可导入时不阻塞测试收集
        pass
    yield
