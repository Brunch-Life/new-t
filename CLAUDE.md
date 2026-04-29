# 新T树洞 Scraper

## 项目概要
爬取新T树洞 (THU Hole) 所有帖子、评论和图片的工具。

## 架构
- 前端: new-t.github.io (React, 源码在 git.thu.monster/newthuhole/hole_thu_frontend)
- 后端: Rust (源码在 git.thu.monster/newthuhole/hole-backend-rust)
- **API 地址**: `https://api.tholeapis.top/_api/v1/` (v2 同域名)
- API 地址是动态的，编码在 GitHub 用户 `hole-thu` 的 bio 里，解码算法在前端 App.js

## API 认证
- Header: `User-Token: <token>`
- Token 从树洞网页登录后获取 (F12 → Network → 请求头里的 User-Token)

## 主要 API 端点
| 端点 | 方法 | 参数 | 用途 |
|------|------|------|------|
| /getlist | GET | p(页码), order_mode, room_id | 分页帖子(25/页) |
| /getone | GET | pid | 单个帖子 |
| /getcomment | GET | pid | 帖子评论 |
| /search | GET | keywords, page, search_mode | 搜索 |
| /getattention | GET | - | 关注的帖子 |
| /getmulti | GET | pids | 批量帖子 |

## 当前状态
- scraper.py 已完成，支持全量爬取和增量监控
- 图片支持: risu.io, imgur, github 等外链图片全部下载
- Mac 上有 launchd 自启配置 (com.newt.scraper.plist / com.newt.dashboard.plist)
- 如需停止自启: `launchctl unload ~/Library/LaunchAgents/com.newt.*.plist`

## 验证 API 地址是否变化
```python
import requests, base64
bio = requests.get("https://api.github.com/users/hole-thu").json()["bio"]
length = len(bio)
sampled = ''.join(bio[(i * 38) % length] for i in range(length))
backend = base64.b64decode(sampled.split('|')[0]).decode()
print(f"https://{backend}/")
```

## 运行
```bash
pip install requests
python3 scraper.py -t TOKEN --once          # 单次全量
python3 scraper.py -t TOKEN -i 120          # 每2分钟增量
```
