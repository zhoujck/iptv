import os
import re
import requests
import subprocess
from urllib.parse import urlparse
from ipaddress import ip_address, IPv4Address, IPv6Address
import concurrent.futures
import time
import threading
from collections import OrderedDict
import warnings

# 抑制SSL警告
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
requests.packages.urllib3.disable_warnings()

# 配置参数
CONFIG_DIR = 'config'
SUBSCRIBE_FILE = os.path.join(CONFIG_DIR, 'subscribe.txt')
DEMO_FILE = os.path.join(CONFIG_DIR, 'demo.txt')
LOCAL_FILE = os.path.join(CONFIG_DIR, 'local.txt')
BLACKLIST_FILE = os.path.join(CONFIG_DIR, 'blacklist.txt')

# 核心修改1：只保留output根目录，不创建子文件夹
OUTPUT_DIR = 'output'
SPEED_LOG = os.path.join(OUTPUT_DIR, 'sort.log')

SPEED_TEST_DURATION = 5
MAX_WORKERS = 10

# 全局变量
failed_domains = set()
log_lock = threading.Lock()
domain_lock = threading.Lock()
counter_lock = threading.Lock()

# 核心修改2：只创建output文件夹，不再创建ipv4/ipv6子文件夹
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --------------------------
# 工具函数
# --------------------------
def write_log(message):
    """线程安全的日志写入"""
    with log_lock:
        with open(SPEED_LOG, 'a', encoding='utf-8') as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")


def get_domain(url):
    """提取域名"""
    try:
        netloc = urlparse(url).netloc
        return netloc.split(':')[0] if ':' in netloc else netloc
    except:
        return None


def update_blacklist(domain):
    """更新黑名单"""
    if domain and domain not in ['localhost', '127.0.0.1']:
        with domain_lock:
            failed_domains.add(domain)


def save_blacklist():
    """保存黑名单到文件"""
    if not failed_domains:
        return
    
    try:
        # 读取已存在的黑名单
        existing = set()
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
                existing = set(line.strip() for line in f if line.strip())
        
        # 找出新增的域名
        new_domains = failed_domains - existing
        if new_domains:
            with open(BLACKLIST_FILE, 'a', encoding='utf-8') as f:
                for domain in sorted(new_domains):
                    f.write(f"{domain}\n")
            print(f"🆕 新增 {len(new_domains)} 个域名到黑名单")
    except Exception as e:
        print(f"⚠️ 保存黑名单失败: {str(e)}")


def get_ip_type(url):
    """安全获取IP类型"""
    try:
        host = urlparse(url).hostname
        if not host:
            return 'ipv4'

        # 尝试解析IP地址类型
        ip = ip_address(host)
        return 'ipv6' if isinstance(ip, IPv6Address) else 'ipv4'
    except ValueError:
        return 'ipv4'
    except Exception as e:
        write_log(f"⚠️ IP类型检测异常: {str(e)} ← {url}")
        return 'ipv4'


def check_dependencies():
    """检查必要依赖"""
    try:
        # 检查ffmpeg
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ 未找到ffmpeg，请先安装ffmpeg并添加到系统PATH")
            return False
    except FileNotFoundError:
        print("❌ 未找到ffmpeg，请先安装ffmpeg并添加到系统PATH")
        return False
    return True


# --------------------------
# 核心逻辑
# --------------------------
def parse_demo_file():
    """解析频道模板文件"""
    print("\n🔍 解析频道模板文件...")
    alias_map = {}
    group_map = {}
    group_order = []
    channel_order = OrderedDict()
    current_group = None

    try:
        with open(DEMO_FILE, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                if line.endswith(',#genre#'):
                    current_group = line.split(',', 1)[0]
                    if current_group not in group_order:
                        group_order.append(current_group)
                        channel_order[current_group] = []
                    print(f"  发现分组 [{current_group}]")
                elif current_group and line:
                    parts = [p.strip() for p in line.split('|')]
                    standard_name = parts[0]
                    if standard_name not in channel_order[current_group]:
                        channel_order[current_group].append(standard_name)

                    for alias in parts:
                        alias_map[alias] = standard_name
                    group_map[standard_name] = current_group

        print(f"✅ 发现 {len(group_order)} 个分组，{len(alias_map)} 个别名")
        return alias_map, group_map, group_order, channel_order

    except FileNotFoundError:
        print(f"⚠️ 模板文件 {DEMO_FILE} 不存在，将使用默认分组")
        return {}, {}, [], OrderedDict()
    except Exception as e:
        print(f"❌ 模板解析失败: {str(e)}")
        return {}, {}, [], OrderedDict()


def fetch_sources():
    """获取订阅源数据"""
    print("\n🔍 获取订阅源...")
    sources = []

    try:
        with open(SUBSCRIBE_FILE, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]

        print(f"  发现 {len(urls)} 个订阅地址")
        for idx, url in enumerate(urls, 1):
            try:
                print(f"\n🌐 正在获取源 ({idx}/{len(urls)})：{url[:50]}...")
                response = requests.get(url, timeout=15, verify=False)
                response.raise_for_status()
                content = response.text

                if '#EXTM3U' in content or url.endswith('.m3u'):
                    parsed = parse_m3u(content)
                    print(f"  解析到 {len(parsed)} 个M3U源")
                    sources.extend(parsed)
                else:
                    parsed = parse_txt(content)
                    print(f"  解析到 {len(parsed)} 个TXT源")
                    sources.extend(parsed)

            except requests.exceptions.RequestException as e:
                print(f"❌ 下载失败: {str(e)}")
            except Exception as e:
                print(f"❌ 处理订阅源异常: {str(e)}")

    except FileNotFoundError:
        print(f"⚠️ 订阅文件 {SUBSCRIBE_FILE} 不存在")

    return sources


def parse_m3u(content):
    """解析M3U格式内容"""
    channels = []
    current = {}
    for line in content.split('\n'):
        line = line.strip()
        if line.startswith('#EXTINF'):
            # 提取频道名称
            match = re.search(r'tvg-name="([^"]*)"', line)
            if not match:
                match = re.search(r',([^,]+)$', line)
            name = match.group(1) if match else '未知频道'
            current = {'name': name.strip(), 'urls': []}
        elif line and not line.startswith('#'):
            if current and line:
                current['urls'].append(line)
                channels.append(current)
                current = {}
    return [{'name': c['name'], 'url': u} for c in channels for u in c['urls']]


def parse_txt(content):
    """解析TXT格式内容"""
    channels = []
    for line in content.split('\n'):
        line = line.strip()
        if ',' in line:
            try:
                name, urls = line.split(',', 1)
                name = name.strip()
                if not name or not urls:
                    continue
                for url in urls.split('#'):
                    clean_url = url.split('$')[0].strip()
                    if clean_url and urlparse(clean_url).scheme:
                        channels.append({'name': name, 'url': clean_url})
            except Exception as e:
                write_log(f"❌ 解析TXT失败: {str(e)} ← {line}")
    return channels


def parse_local():
    """解析本地源文件"""
    print("\n🔍 解析本地源...")
    sources = []
    try:
        with open(LOCAL_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if ',' in line:
                    try:
                        name, urls = line.split(',', 1)
                        name = name.strip()
                        if not name or not urls:
                            continue
                        for url in urls.split('#'):
                            parts = url.split('$', 1)
                            clean_url = parts[0].strip()
                            if clean_url and urlparse(clean_url).scheme:
                                source = {
                                    'name': name,
                                    'url': clean_url,
                                    'whitelist': len(parts) > 1
                                }
                                sources.append(source)
                    except Exception as e:
                        write_log(f"❌ 解析本地文件失败: {str(e)} ← {line}")
        print(f"✅ 找到 {len(sources)} 个本地源")
    except FileNotFoundError:
        print(f"⚠️ 本地源文件 {LOCAL_FILE} 不存在")
    return sources


def read_blacklist():
    """读取黑名单列表"""
    try:
        with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
            return [line.strip().lower() for line in f if line.strip()]
    except FileNotFoundError:
        return []


def filter_sources(sources, blacklist):
    """过滤黑名单源"""
    print("\n🔍 过滤黑名单...")
    filtered = []
    
    for s in sources:
        url = s['url']
        # URL格式校验
        parsed_url = urlparse(url)
        if not parsed_url.scheme:
            write_log(f"🚫 无效URL格式: {url}")
            continue

        if s.get('whitelist', False):
            filtered.append(s)
            continue

        # 检查黑名单
        domain = get_domain(url)
        if domain and (domain.lower() in blacklist or 
                      any(kw in url.lower() for kw in blacklist)):
            write_log(f"🚫 拦截黑名单: {url}")
            continue

        filtered.append(s)

    print(f"✅ 保留 {len(filtered)}/{len(sources)} 个源")
    return filtered


def test_rtmp(url):
    """RTMP推流检测"""
    try:
        result = subprocess.run(
            ['ffmpeg', '-i', url, '-t', '1', '-v', 'error', '-f', 'null', '-'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
            text=True
        )
        if result.returncode == 0:
            write_log(f"RTMP检测成功: {url}")
            return 100  # 返回基础速度值
        else:
            error_msg = result.stderr[:100] if result.stderr else "未知错误"
            write_log(f"RTMP检测失败: {url} | {error_msg}")
            return 0
    except subprocess.TimeoutExpired:
        write_log(f"RTMP检测超时: {url}")
        return 0
    except Exception as e:
        write_log(f"RTMP检测异常: {url} | {str(e)}")
        return 0


def test_speed(url):
    """增强版测速函数"""
    try:
        start_time = time.time()

        # RTMP协议处理
        if url.startswith(('rtmp://', 'rtmps://')):
            return test_rtmp(url)

        # HTTP协议处理
        if not url.startswith(('http://', 'https://')):
            write_log(f"⚠️ 跳过非常规协议: {url}")
            return 0

        with requests.Session() as session:
            response = session.get(url,
                                   stream=True,
                                   timeout=(3.05, SPEED_TEST_DURATION + 2),
                                   allow_redirects=True,
                                   verify=False,
                                   headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
            response.raise_for_status()

            total_bytes = 0
            data_start = time.time()
            
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    total_bytes += len(chunk)
                # 超时控制
                if (time.time() - data_start) >= SPEED_TEST_DURATION:
                    break

            duration = max(time.time() - data_start, 0.001)
            speed = (total_bytes / 1024) / duration  # KB/s
            
            log_msg = (f"✅ 测速成功: {url[:100]} | "
                       f"速度: {speed:.2f}KB/s | 数据量: {total_bytes / 1024:.1f}KB | "
                       f"耗时: {time.time() - start_time:.2f}s")
            write_log(log_msg)
            return speed

    except requests.exceptions.RequestException as e:
        domain = get_domain(url)
        update_blacklist(domain)
        log_msg = f"❌ 测速失败: {url[:100]} | 错误: {str(e)} | 域名: {domain}"
        write_log(log_msg)
        return 0
    except Exception as e:
        domain = get_domain(url)
        update_blacklist(domain)
        log_msg = f"❌ 测速异常: {url[:100]} | 错误: {str(e)} | 域名: {domain}"
        write_log(log_msg)
        return 0


def process_sources(sources):
    """处理所有源并进行测速"""
    total = len(sources)
    if total == 0:
        print("\n⚠️ 没有可处理的源")
        return []
        
    print(f"\n🔍 开始检测 {total} 个源 (并发数: {MAX_WORKERS})")
    processed = []
    processed_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交任务
        future_to_source = {}
        for s in sources:
            future = executor.submit(
                lambda s: (s['name'], s['url'], test_speed(s['url']), get_ip_type(s['url'])), s)
            future_to_source[future] = s

        # 处理结果
        for future in concurrent.futures.as_completed(future_to_source):
            try:
                name, url, speed, ip_type = future.result()
                with counter_lock:
                    processed_count += 1
                    progress = f"[{processed_count}/{total}]"

                # 格式化输出
                speed_str = f"{speed:>7.2f}KB/s"
                name_display = name[:20].ljust(20)
                print(f"{progress} 📊 频道: {name_display} | 速度: {speed_str} | 类型: {ip_type.upper()} | {url[:80]}...")
                
                # 只保留速度大于0的源
                if speed > 0:
                    processed.append((name, url, speed, ip_type))
                    
            except Exception as e:
                source = future_to_source[future]
                error_msg = f"⚠️ 处理异常: {str(e)} ← {source['url'][:50]}..."
                print(error_msg)
                write_log(error_msg)

    # 保存黑名单
    save_blacklist()
    
    print(f"\n✅ 检测完成 - 有效源: {len(processed)}/{total}")
    return processed


def organize_channels(processed, alias_map, group_map):
    """整理频道数据"""
    print("\n📚 整理频道数据...")
    organized = {'ipv4': OrderedDict(), 'ipv6': OrderedDict()}

    for name, url, speed, ip_type in processed:
        # 验证IP类型
        if ip_type not in ('ipv4', 'ipv6'):
            ip_type = 'ipv4'

        # 标准化频道名称
        std_name = alias_map.get(name, name)
        group = group_map.get(std_name, '其他')

        # 初始化结构
        if group not in organized[ip_type]:
            organized[ip_type][group] = OrderedDict()
        if std_name not in organized[ip_type][group]:
            organized[ip_type][group][std_name] = []

        # 添加源（去重）
        if (url, speed) not in organized[ip_type][group][std_name]:
            organized[ip_type][group][std_name].append((url, speed))

    return organized


def finalize_output(organized, group_order, channel_order):
    """生成输出文件 - 核心修改：文件直接放在output目录，文件名带ipv4/ipv6前缀"""
    print("\n📂 生成结果文件...")
    
    for ip_type in ['ipv4', 'ipv6']:
        # 核心修改3：定义输出文件路径，直接放在output目录下
        txt_file = os.path.join(OUTPUT_DIR, f'{ip_type}_result.txt')
        m3u_file = os.path.join(OUTPUT_DIR, f'{ip_type}_result.m3u')
        
        txt_lines = []
        # M3U头部 - 修复EPG链接格式
        m3u_lines = [
            '#EXTM3U x-tvg-url="https://gh.catmak.name/https://raw.githubusercontent.com/Guovin/iptv-api/refs/heads/master/output/epg/epg.gz"'
        ]

        # 按模板顺序处理分组
        for group in group_order:
            if group not in organized[ip_type]:
                continue

            # TXT格式分组标记
            txt_lines.append(f"{group},#genre#")

            # 处理模板频道
            for channel in channel_order.get(group, []):
                if channel not in organized[ip_type][group]:
                    continue

                # 按速度降序排序，取前10个
                urls = sorted(organized[ip_type][group][channel], 
                             key=lambda x: x[1], reverse=True)[:10]
                
                if urls:
                    # TXT格式：频道名,地址1#地址2#地址3
                    txt_urls = [u[0] for u in urls]
                    txt_lines.append(f"{channel},{'#'.join(txt_urls)}")
                    
                    # M3U格式 - 修复语法错误
                    for url in txt_urls:
                        # 修复logo链接和格式
                        logo_url = f"https://gh.catmak.name/https://raw.githubusercontent.com/fanmingming/live/main/tv/{channel}.png"
                        m3u_lines.append(
                            f'#EXTINF:-1 tvg-name="{channel}" tvg-logo="{logo_url}" group-title="{group}",{channel}'
                        )
                        m3u_lines.append(url)

            # 处理额外频道（不在模板中的）
            extra_channels = [c for c in organized[ip_type][group] if c not in channel_order.get(group, [])]
            for channel in sorted(extra_channels, key=lambda x: x.lower()):
                urls = sorted(organized[ip_type][group][channel], 
                             key=lambda x: x[1], reverse=True)[:10]
                if urls:
                    txt_urls = [u[0] for u in urls]
                    txt_lines.append(f"{channel},{'#'.join(txt_urls)}")
                    
                    for url in txt_urls:
                        m3u_lines.append(
                            f'#EXTINF:-1 tvg-name="{channel}" group-title="{group}",{channel}'
                        )
                        m3u_lines.append(url)

        # 处理"其他"分组
        if '其他' in organized[ip_type]:
            txt_lines.append("其他,#genre#")
            for channel in sorted(organized[ip_type]['其他'].keys(), key=lambda x: x.lower()):
                urls = sorted(organized[ip_type]['其他'][channel], 
                             key=lambda x: x[1], reverse=True)[:10]
                if urls:
                    txt_urls = [u[0] for u in urls]
                    txt_lines.append(f"{channel},{'#'.join(txt_urls)}")
                    
                    for url in txt_urls:
                        m3u_lines.append(
                            f'#EXTINF:-1 tvg-name="{channel}" group-title="其他",{channel}'
                        )
                        m3u_lines.append(url)

        # 写入文件
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(txt_lines))
        
        with open(m3u_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(m3u_lines))

        print(f"  ✅ {ip_type.upper()} 文件生成完成：")
        print(f"     - TXT: {txt_file}")
        print(f"     - M3U: {m3u_file}")


if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("🎬 IPTV直播源处理脚本（增强版）")
    print("=" * 60)
    
    # 检查依赖
    if not check_dependencies():
        exit(1)

    # 初始化日志文件
    with open(SPEED_LOG, 'w', encoding='utf-8') as f:
        f.write(f"IPTV测速日志 - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 50 + "\n")

    try:
        # 1. 解析模板文件
        alias_map, group_map, group_order, channel_order = parse_demo_file()
        
        # 2. 获取所有源
        subscribe_sources = fetch_sources()
        local_sources = parse_local()
        all_sources = subscribe_sources + local_sources
        print(f"\n📥 总计获取 {len(all_sources)} 个源")

        if not all_sources:
            print("⚠️ 未获取到任何源，程序退出")
            exit(0)

        # 3. 过滤源
        blacklist = read_blacklist()
        filtered_sources = filter_sources(all_sources, blacklist)

        # 4. 测速处理
        processed_sources = process_sources(filtered_sources)

        # 5. 整理频道
        organized_channels = organize_channels(processed_sources, alias_map, group_map)

        # 6. 生成输出文件
        finalize_output(organized_channels, group_order, channel_order)

        print("\n" + "=" * 60)
        print("🎉 所有处理完成！结果文件列表：")
        print(f"   - IPv4 TXT: {os.path.join(OUTPUT_DIR, 'ipv4_result.txt')}")
        print(f"   - IPv4 M3U: {os.path.join(OUTPUT_DIR, 'ipv4_result.m3u')}")
        print(f"   - IPv6 TXT: {os.path.join(OUTPUT_DIR, 'ipv6_result.txt')}")
        print(f"   - IPv6 M3U: {os.path.join(OUTPUT_DIR, 'ipv6_result.m3u')}")
        print(f"   - 测速日志: {SPEED_LOG}")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断操作")
        save_blacklist()
    except Exception as e:
        print(f"\n❌ 程序异常: {str(e)}")
        write_log(f"程序异常: {str(e)}")
        save_blacklist()
