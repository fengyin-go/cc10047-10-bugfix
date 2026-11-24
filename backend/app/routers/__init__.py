"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, chains, workbench

# workbench 的富动作路径更具体，放在通用 resource 之前注册
ROUTERS = [workbench, resource, chains]
