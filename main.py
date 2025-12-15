import os
import re
import requests
import json
import time
from datetime import datetime, timedelta
from collections import defaultdict

# ================= 配置区 =================
APP_ID = "cli_a9a427abc73a1bc7"
APP_SECRET = "xza3K8d65ks5DcN9DG1P7dTAXKNYLz5E"

# 之前更新的表格 Token
SPREADSHEET_TOKEN = "Y7sEsZsjrhcQyvt0U7HcyqGPnNh"

# ✅ 变更点：已更新为新的企业微信 Webhook
WECOM_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=9f59729a-0140-4044-88a2-026996d894bb"

# 之前更新的子表 ID
TARGET_SHEET_IDS = ["Z7k4T5"]

class MonitorBot:
    def __init__(self):
        self.token = ""
        self.sheet_names = {} 
        self.scanned_list = []
        self.error_count = 0    

    # ⚠️ 注意：读取飞书表格数据仍需保留此认证函数
    def get_tenant_access_token(self):
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        try:
            resp = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}).json()
            if resp.get("code") == 0:
                self.token = resp.get("tenant_access_token")
            else:
                print(f"❌ 飞书API认证失败: {resp}")
        except Exception as e:
            print(f"❌ 网络请求错误: {e}")

    def load_all_sheet_names(self):
        if not self.token: self.get_tenant_access_token()
        url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{SPREADSHEET_TOKEN}/sheets/query"
        headers = {"Authorization": f"Bearer {self.token}"}
        resp = requests.get(url, headers=headers).json()
        if resp.get("code") == 0:
            sheets = resp.get("data", {}).get("sheets", [])
            for sheet in sheets:
                self.sheet_names[sheet.get("sheet_id")] = sheet.get("title", "未命名")
            print(f"📚 表格名加载完毕")

    def clean_text(self, cell_data):
        if cell_data is None: return ""
        if isinstance(cell_data, str): return cell_data
        
        def extract_segment(seg):
            if not isinstance(seg, dict): return str(seg)
            if 'fileToken' in seg or 'image_key' in seg or seg.get('type') in ['embed-image', 'file', 'mention']:
                return ""
            return seg.get('text', "")

        if isinstance(cell_data, list):
            text_list = []
            for segment in cell_data:
                text_list.append(extract_segment(segment))
            return "".join(text_list)
            
        if isinstance(cell_data, dict):
            return extract_segment(cell_data)

        return str(cell_data)

    def is_safe_content(self, text):
        safe_words = [
            "通过", "完成", "无需", "pass", "ok", "done", 
            "提交下游", "已提交下游", "交下游"
        ]
        text_lower = text.lower()
        return any(w in text_lower for w in safe_words)

    def is_noise(self, text):
        t = text.strip().lower()
        if not t: return True
        if t in ["-", "/", "\\", "."]: return True
        if re.match(r'^[cC]\d+$', t): return True
        if re.search(r'\d+\s*[xX*]\s*\d+', t): return True
        return False

    def has_chinese(self, text):
        return bool(re.search(r'[\u4e00-\u9fa5]', text))

    def get_column_letter(self, col_idx):
        if col_idx < 26: return chr(65 + col_idx)
        else: return 'A' + chr(65 + (col_idx - 26))

    def find_shot_number(self, row):
        scan_limit = min(len(row), 5)
        shot_pattern = re.compile(r'(?i)[a-z]+[-_]?\d+') 
        for i in range(scan_limit):
            text = self.clean_text(row[i]).strip()
            if not text: continue
            if text in ["镜号", "镜头", "序号"]: continue 
            if shot_pattern.search(text):
                return text
        return None

    def find_stage_name_dynamic(self, col_idx, header1, header2):
        skip_keywords = ["反馈", "说明", "需求", "状态", "CK", "Time", "当前", "进度", "素材"]
        for j in range(col_idx, -1, -1):
            h1 = self.clean_text(header1[j] if j < len(header1) else "").strip()
            if not h1: continue
            if any(k in h1 for k in skip_keywords): continue
            return h1
        return "未知环节"

    def scan_row_full(self, row, now, header1, header2):
        total_cols = len(row)
        issues = []
        
        for i in range(total_cols):
            text = self.clean_text(row[i]).strip()
            if not text: continue
            if self.is_noise(text): continue
            if self.is_safe_content(text): continue
            if not self.has_chinese(text): continue 

            match = re.search(r'(0[1-9]|1[0-2]|[1-9])[\.\-\/]?([0-2][0-9]|3[01]|[1-9])', text)
            
            if match:
                h1 = self.clean_text(header1[i] if i < len(header1) else "").strip()
                h2 = self.clean_text(header2[i] if i < len(header2) else "").strip()
                full_header = h1 + h2
                
                if ("状态" not in full_header and "进度" not in full_header): continue 
                if "反馈" in full_header: continue 

                try:
                    m_str, d_str = match.group(1), match.group(2)
                    month, day = int(m_str), int(d_str)
                    if month > 12 or day > 31: continue

                    year = now.year
                    if now.month == 12 and month == 1: year += 1
                    elif now.month == 1 and month == 12: year -= 1
                    
                    target_date = datetime(year, month, day)
                    days_diff = (now.date() - target_date.date()).days
                    
                    is_today = (days_diff == 0)
                    is_yesterday = (days_diff == 1)
                    
                    stage_name = self.find_stage_name_dynamic(i, header1, header2)

                    if is_today or is_yesterday:
                        issues.append((text, i, stage_name, 'recent', days_diff)) 
                    elif days_diff > 1:
                        issues.append((text, i, stage_name, 'severe', days_diff)) 
                except ValueError: continue
        return issues

    def process_single_sheet(self, current_sheet_id):
        sheet_name = self.sheet_names.get(current_sheet_id, f"表格({current_sheet_id})")
        print(f"\n🔍 扫描 [{sheet_name}] ...")
        
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{current_sheet_id}!A1:AZ800"
        headers = {"Authorization": f"Bearer {self.token}"}
        resp = requests.get(url, headers=headers).json()
        
        if resp.get("code") != 0: return
        rows = resp.get("data", {}).get("valueRange", {}).get("values", [])
        if not rows or len(rows) < 2: return 
        
        self.scanned_list.append(sheet_name)
        header1 = rows[0]
        header2 = rows[1] if len(rows) > 1 else []
        
        utc_now = datetime.utcnow()
        beijing_now = utc_now + timedelta(hours=8)
        now = beijing_now 
        
        recent_groups = defaultdict(list)
        backlog_groups = defaultdict(list)

        data_rows = rows[2:] if len(rows) > 2 else []

        for i, row in enumerate(data_rows):
            real_row_num = i + 3
            display_name = self.find_shot_number(row)
            
            is_unknown = False
            if not display_name:
                line_content = "".join([self.clean_text(c) for c in row]).strip()
                if not line_content: continue 
                display_name = "未知任务"
                is_unknown = True
            
            row_issues = self.scan_row_full(row, now, header1, header2)
            if not row_issues: continue

            for status_text, col_idx, stage_name, issue_type, days in row_issues:
                # 坐标尾巴：仅在未知时显示
                coord_info = ""
                if is_unknown:
                    col_char = self.get_column_letter(col_idx)
                    coord_info = f" ({col_char}{real_row_num})"
                
                display_text = f"**[{stage_name}] {display_name}**: {status_text}{coord_info}"
                
                if issue_type == 'recent':
                    self.error_count += 1
                    recent_groups[stage_name].append(f"🟠 {display_text} (近期变动)")
                elif issue_type == 'severe':
                    self.error_count += 1
                    backlog_groups[stage_name].append(f"🔴 {display_text} (超期{days}天)")

        # 组装消息列表 (含空行)
        final_msg_list = []
        
        if recent_groups:
            final_msg_list.append("⚡ **今日/昨日最新变动 (请优先处理)：**")
            for stage, items in recent_groups.items():
                final_msg_list.extend(items)
                final_msg_list.append("") 
            final_msg_list.append("----------------------------------") 
        
        if backlog_groups:
            final_msg_list.append("📉 **历史积压与异常风险：**")
            for stage, items in backlog_groups.items():
                final_msg_list.extend(items)
                final_msg_list.append("") 

        # ✅ 变更点：仅发送企微通道
        self.send_wecom_alert(sheet_name, final_msg_list, current_sheet_id)

    # 🚀 企微发送函数
    def send_wecom_alert(self, sheet_name, msgs, sheet_id):
        if not msgs: return
        valid_lines = [m for m in msgs if m and m.strip()]
        if len(valid_lines) <= 2: return 

        print(f"🚀 发送企微: {sheet_name}")
        
        # 企微消息分片 (防止超长)
        CHUNK_SIZE = 20 # 每次发20行左右
        for i in range(0, len(msgs), CHUNK_SIZE):
            chunk = msgs[i : i + CHUNK_SIZE]
            content_str = "\n".join(chunk)
            
            # 第一条带标题
            title = f"## 🚨 进度异常日报 | {sheet_name}\n" if i == 0 else ""
            
            # 最后一条带链接
            footer = ""
            if (i + CHUNK_SIZE) >= len(msgs):
                sheet_url = f"https://feishu.cn/sheets/{SPREADSHEET_TOKEN}?sheet={sheet_id}"
                footer = f"\n\n> 🔗 [点击进入飞书表格]({sheet_url})"

            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "content": f"{title}{content_str}{footer}"
                }
            }
            try:
                requests.post(WECOM_WEBHOOK, json=payload)
                time.sleep(0.5)
            except: pass

    def send_summary(self):
        print("发送汇总...")
        # ✅ 变更点：仅发送企微汇总
        wc_content = f"## ✅ 巡检完成日报\n**共扫描 {len(self.scanned_list)} 个表格**\n🚫 **发现风险项：** <font color=\"warning\">{self.error_count}</font> 个"
        try: 
            time.sleep(0.5)
            requests.post(WECOM_WEBHOOK, json={"msgtype": "markdown", "markdown": {"content": wc_content}})
        except: pass

    def run(self):
        print("🤖 V49.2 (Single Channel: WeCom Only)...")
        self.load_all_sheet_names()
        for sheet_id in TARGET_SHEET_IDS:
            try:
                self.process_single_sheet(sheet_id)
                time.sleep(1)
            except Exception as e:
                print(f"⚠️ 扫描出错 [{sheet_id}]: {e}")
        self.send_summary()
        print("✅ 任务全部结束")

if __name__ == "__main__":
    MonitorBot().run()