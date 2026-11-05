"""从一个姿态到另一个姿态时"先动谁、后动谁"的规则。纯计算，不碰硬件，两台臂通用（机械尺寸相同）。

单位都是 ticks（一圈 4096 格）。规则来自 Seeed 臂上操作员手动站立序列的走法及其镜像：
  · 腕：要抬头（ticks 变小）就最先动，要低头就最后动——腕往下会把夹爪戳向桌面；
  · 底座和腕旋：在大臂更竖直的那一端转，避免臂放低时横扫桌面；
  · 肩和肘一起走，但折叠那一侧的 HEAD 由肩单独走：展开肘时肩先走，折叠肘时肩最后走。

⚠ 这只是排序，不知道桌面和周围物体在哪里。真机上只在个别姿态之间试过，没有系统验证。
"""
ARM_JOINTS = ['shoulder_pan', 'shoulder_lift', 'elbow_flex', 'wrist_flex', 'wrist_roll']
HEAD = 120            # 肩关节在"折叠那一侧"单独先走或后走的幅度
UPRIGHT = 2048        # 量程中点，大臂大致竖直；只用来决定底座何时转


def route(now, target, arrived):
    """从 now 到 target 的路点列表 [{关节: ticks}, ...]。离目标不到 arrived 的关节不安排动作。"""
    go = {name: t for name, t in target.items() if abs(t - now[name]) >= arrived}
    pick = lambda *names: {name: go[name] for name in names if name in go}
    lift0, lift1 = now['shoulder_lift'], target['shoulder_lift']
    head = max(-HEAD, min(HEAD, lift1 - lift0))
    if not {'shoulder_lift', 'elbow_flex'} <= go.keys():
        core = [pick('shoulder_lift', 'elbow_flex')]
    elif go['elbow_flex'] < now['elbow_flex']:          # 肘要展开：肩先走
        core = [{'shoulder_lift': lift0 + head}, pick('shoulder_lift', 'elbow_flex')]
    else:                                               # 肘要折叠：肩最后走
        core = [pick('elbow_flex'), pick('shoulder_lift')]
        if abs(lift1 - lift0) > HEAD:
            core[0]['shoulder_lift'] = lift1 - head
    tip_up = 'wrist_flex' in go and go['wrist_flex'] < now['wrist_flex']
    turn_first = abs(lift0 - UPRIGHT) < abs(lift1 - UPRIGHT)
    wrist, turn = pick('wrist_flex'), pick('shoulder_pan', 'wrist_roll')
    order = [wrist if tip_up else {}, turn if turn_first else {}, *core,
             {} if turn_first else turn, {} if tip_up else wrist]
    return [waypoint for waypoint in order if waypoint]


def steps(now, target, chunk, arrived):
    """把路点摊平成单关节动作 [(关节, 目标 ticks), ...]，每步不超过 chunk。

    肩和肘"一起走"的路点变成两者轮流各走一小段，次序沿用路点里的次序（展开时肩在前，折叠时肘在前）。
    """
    at = {name: now[name] for name in ARM_JOINTS}
    out = []
    for waypoint in route(now, target, arrived):
        start = dict(at)
        rounds = max(1, max(-(-abs(goal - start[name]) // chunk) for name, goal in waypoint.items()))
        for i in range(1, rounds + 1):
            for name, goal in waypoint.items():
                stop = start[name] + round((goal - start[name]) * i / rounds)
                if abs(stop - at[name]) >= arrived:
                    out.append((name, stop))
                    at[name] = stop
    return out
