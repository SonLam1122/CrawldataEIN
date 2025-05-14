# Massachusetts  https://corp.sec.state.ma.us/corpweb/CorpSearch/CorpSearch.aspx done
# West Virginia  https://apps.sos.wv.gov/business/corporations/
# Wyoming  https://wyobiz.wyo.gov/Business/FilingSearch.aspx
import threading
import time
import os
import shutil
from playwright.sync_api import sync_playwright
import sys
import pandas as pd
from queue import Queue
import random
from openpyxl import load_workbook
######################### Biến #########################
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
excel_path = os.path.join(BASE_DIR, "DataEIN.xlsx")
all_sheets = pd.ExcelFile(excel_path).sheet_names
proxy_lock = threading.Lock()
proxy_index = 0
proxies = []
temp_path = "DataEIN_temp.xlsx"
########################################################

######################### Danh sách các sheet #########################
print("Danh sách các sheet có sẵn trong file Excel:")
for idx, sheet in enumerate(all_sheets):
    print(f"{idx + 1}. {sheet}")
# Cho người dùng chọn
while True:
    try:
        selected_index = int(input("Nhập số thứ tự sheet muốn xử lý: ")) - 1
        if 0 <= selected_index < len(all_sheets):
            SHEET_NAME = all_sheets[selected_index]
            break
        else:
            print("❌ Số không hợp lệ. Vui lòng chọn lại.")
    except ValueError:
        print("❌ Vui lòng nhập số.")
        
# Đọc dữ liệu
df = pd.read_excel(excel_path, sheet_name=SHEET_NAME)
if "Address" not in df.columns:
    df["Address"] = ""
else:
    df["Address"] = df["Address"].fillna("")
chunk_size = 10
chunk_queue = Queue()
for i in range(0, len(df), chunk_size):
    chunk_queue.put((df.iloc[i:i + chunk_size].copy(), i // chunk_size))
lock = threading.Lock()

######################### Hàm hỗ trợ #########################
def create_profile(profile_name):
    profile_path = os.path.abspath(f'profiles/{profile_name}')
    os.makedirs('profiles', exist_ok=True)
    if os.path.exists(profile_path):
        shutil.rmtree(profile_path, ignore_errors=True)
    os.makedirs(profile_path, exist_ok=True)
    return profile_path
def load_proxies(filename="proxy.txt"):
    if not os.path.exists(filename):
        print("⚠️ Tệp proxy không tồn tại!")
        return []
    with open(filename, "r") as file:
        proxies = [line.strip() for line in file if line.strip()]
    random.shuffle(proxies) 
    return proxies
def get_next_valid_proxy():
    global proxy_index, proxies
    if not proxies:
        return None

    with proxy_lock:
        if proxy_index >= len(proxies):
            random.shuffle(proxies)
            proxy_index = 0
        proxy = proxies[proxy_index]
        proxy_index += 1
        return proxy

batch_updates = []
batch_lock = threading.Lock()
BATCH_SIZE = 20  

def update_address_safely(df_chunk, i, row, address):
    global batch_updates

    df_chunk.iat[i, df_chunk.columns.get_loc("Address")] = address

    with batch_lock:
        batch_updates.append((i, row, address))

        if len(batch_updates) < BATCH_SIZE:
            return True

    return flush_batch_updates(df_chunk)

def flush_batch_updates(df_chunk):
    global batch_updates

    with batch_lock:
        if not batch_updates:
            return True

        current_batch = batch_updates.copy()
        batch_updates = []

    max_retries = 3
    retry_delay = 1

    for attempt in range(max_retries):
        temp_file = None
        try:
            temp_file = f"DataEIN_temp_{threading.get_ident()}_{time.time()}.xlsx"

            with lock:  # Đảm bảo chỉ một luồng ghi vào tệp tại một thời điểm
                if not os.path.exists("DataEIN.xlsx"):
                    df.to_excel("DataEIN.xlsx", sheet_name=SHEET_NAME, index=False)
                    return True

                with pd.ExcelWriter(temp_file, engine='openpyxl') as writer:
                    with pd.ExcelFile("DataEIN.xlsx") as reader:
                        for sheet_name in reader.sheet_names:
                            if sheet_name != SHEET_NAME:
                                pd.read_excel(reader, sheet_name=sheet_name).to_excel(
                                    writer, sheet_name=sheet_name, index=False)

                    for i, row, address in current_batch:
                        if i >= len(df_chunk):
                            print(f"⚠️ Chỉ mục {i} vượt quá kích thước của DataFrame (kích thước: {len(df_chunk)})")
                            continue
                        df.loc[row.name, "Address"] = address
                        df_chunk.iat[i, df_chunk.columns.get_loc("Address")] = address

                    try:
                        df.to_excel(writer, sheet_name=SHEET_NAME, index=False)
                    except Exception as e:
                        print(f"⚠️ Lỗi khi ghi tệp: {e}")
                        raise

                os.replace(temp_file, "DataEIN.xlsx")

            for i, row, address in current_batch:
                print(f"[✓] {row['Business Name']} → {address}")

            return True

        except Exception as e:
            print(f"⚠️ Lỗi khi ghi batch (lần {attempt + 1}): {str(e)}")
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as e:
                    print(f"⚠️ Không thể xóa tệp tạm thời: {temp_file} - {e}")
            time.sleep(retry_delay)
            continue

    print(f"❌ Không thể ghi batch sau {max_retries} lần thử")
    return False

###############################################################

######################### Setup #########################
def setup_browser(profile_path: str, proxy: str = None, p: sync_playwright = None):
    proxy_config = {"server": f'socks5://{proxy}'} if proxy else None
    try:
        browser = p.chromium.launch_persistent_context(
            profile_path,
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
                "--disable-web-security",
                "--no-sandbox"
            ],
            proxy=proxy_config
        )
    except Exception as e:
        print(f"❌ Lỗi khi khởi động trình duyệt: {str(e)}")
        return None, None

    page = browser.new_page()
    page.set_extra_http_headers({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    })
    page.evaluate("() => { Object.defineProperty(navigator, 'webdriver', { get: () => undefined }) }")
    page.set_viewport_size({"width": 1280, "height": 720})
    
    return browser, page
##########################################################

######################### Auto #########################
def run_browser(profile_name, df_chunk, chunk_index, proxy=None):
    with sync_playwright() as p:
        profile_path = create_profile(profile_name)
        browser, page = setup_browser(profile_path,proxy, p)
        if browser and page:
            ##################### Thêm sheet sử lý vòa đây #####################
            if SHEET_NAME.lower() == "massachusetts":
                auto_massachusetts(page, profile_name, df_chunk, chunk_index)
            elif SHEET_NAME.lower() == "west virginia":
                auto_west_virginia(page, profile_name, df_chunk, chunk_index)
            elif SHEET_NAME.lower() == "wyoming":
                auto_wyoming(page, profile_name, df_chunk, chunk_index)
            else:
                print(f"❌ Không nhận diện được sheet: {SHEET_NAME}")
            page.close()
            browser.close()
        try:
            shutil.rmtree(profile_path, ignore_errors=True)
        except Exception as e:
            print(f"⚠️ Không thể xóa profile {profile_name}: {e}")
############################## viết hàm get address ##############################
def auto_west_virginia(page, profile_name, df_chunk, chunk_index):
    # TODO: Viết hàm này 
    pass

def auto_wyoming(page, profile_name, df_chunk, chunk_index):
    # TODO: Viết hàm này
    pass

def auto_massachusetts(page, profile_name, df_chunk, chunk_index):
    for i in range(len(df_chunk)):
        row = df_chunk.iloc[i]
        if row["Address"].strip() != "":
            continue
        name = str(row["Business Name"])
        try:
            page.goto("https://corp.sec.state.ma.us/corpweb/CorpSearch/CorpSearch.aspx", timeout=60000)
            
            page.fill("input[name='ctl00$MainContent$txtEntityName']", name)
            page.click("input[name='ctl00$MainContent$btnSearch']")

            page.wait_for_timeout(3000)

            if page.is_visible("#MainContent_lblMessage"):
                msg = page.text_content("#MainContent_lblMessage")
                address = msg.strip()
            else:
                page.wait_for_selector("tr.GridRow", timeout=10000)
                address_html = page.query_selector("tr.GridRow td:nth-child(4)").inner_html()
                address = " ".join(address_html.replace("<br>", " ").split())
                
            update_address_safely(df_chunk, i, row, address)
        except Exception as e:
            print(f"[X] {name} → Lỗi: {e}")
            continue
#########################################################

############################################################## main ##############################################################
def start_browser_loop(thread_id):
    while not chunk_queue.empty():
        try:
            df_chunk, chunk_index = chunk_queue.get_nowait()

            df_to_process = df_chunk[df_chunk["Address"].isna() | (df_chunk["Address"].astype(str).str.strip() == "")]
            
            if len(df_to_process) == 0:
                continue
                
            proxy = get_next_valid_proxy()
            if not proxy:
                print(f"⚠️ Không có proxy")
                time.sleep(5)
                chunk_queue.put((df_chunk, chunk_index)) 
                continue

            profile_name = f"profile_{thread_id}_chunk_{chunk_index}"
            
            run_browser(profile_name, df_to_process, chunk_index, proxy)
            
            time.sleep(2)
            
        except Exception as e:
            print(f"⚠️ Luồng {thread_id} gặp lỗi: {e}")
            time.sleep(5)

if __name__ == "__main__":
    if os.path.exists("profiles"):
        shutil.rmtree("profiles", ignore_errors=True)
    
    os.makedirs("profiles", exist_ok=True)
    
    proxies = load_proxies()
    if not proxies:
        print("⚠️ Không tìm thấy proxy, sẽ chạy không có proxy!")
    
    max_threads = min(os.cpu_count() * 2, 32)  # Giới hạn tối đa 32 luồng
    num_threads = min(int(input("Nhập số luồng muốn chạy: ")), max_threads)
    print(f"Khởi chạy {num_threads} luồng (Tối đa có thể chạy: {max_threads} luồng)")
    
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=start_browser_loop, args=(i,))
        threads.append(thread)
        thread.start()

    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("⛔ Đã dừng bằng tay.")
        sys.exit(0)