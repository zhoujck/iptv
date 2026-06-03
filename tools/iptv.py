import os
import re
import requests
import subprocess
from urllib.parse import urlparse, urlunparse
from ipaddress import ip_address, IPv4Address, IPv6Address
import concurrent.futures
import time
import threading
from collections import OrderedDict
import ssl
import socket
import hashlib
import warnings

# 抑制SSL警告
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
requests.packages.urllib3.disable_warnings()

# 配置参数
CONFIG_DIR = 'tools'
SUBSCRIBE_FILE = os.path.join(CONFIG_DIR, 'subscribe.txt')
DEMO_FILE = os.path.join(CONFIG_DIR, 'demo.txt')
LOCAL_FILE = os.path.join(CONFIG_DIR, 'local.txt')
BLACKLIST_FILE = os.path.join(CONFIG_DIR, 'blacklist.txt')
RUN_COUNT_FILE = os.path.join(CONFIG_DIR, 'run_count.txt')

# 输出目录 - 保持原目录结构
OUTPUT_DIR = 'output'
SPEED_LOG = os.path.join(OUTPUT_DIR, 'sort.log')

SPEED_TEST_DURATION = 5
MAX_WORKERS = 10
HTTPS_VERIFY = False
SPEED_THRESHOLD = 300   # 速度阈值 KB/s
RESET_COUNT = 12        # 运行12次后重置黑名单
TEST_ALL_SOURCES = False # True=检测所有源, False=只检测模板内的源

# 全局变量
failed_domains = set()
log_lock = threading.Lock()
domain_lock = threading.Lock()
counter_lock = threading.Lock()
url_cache_lock = threading.Lock()
url_cache = set()

os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --------------------------
# 运行次数管理
# --------------------------
def manage_run_count():
    """管理运行次数并在达到阈值时清空黑名单"""
    try:
        if os.path.exists(RUN_COUNT_FILE):
            with open(RUN_COUNT_FILE, 'r') as f:
                current_count = int(f.read().strip())
        else:
            current_count = 0

        current_count += 1
        print(f"🔢 当前是第 {current_count} 次运行")

        if current_count >= RESET_COUNT:
            print("🔄 达到运行阈值，清空黑名单文件")
            if os.path.exists(BLACKLIST_FILE):
                with open(BLACKLIST_FILE, 'w') as f:
                    f.write('')
                print("✅ 黑名单文件已清空")
            current_count = 0

        with open(RUN_COUNT_FILE, 'w') as f:
            f.write(str(current_count))

        return current_count

    except Exception as e:
        print(f"❌ 运行次数管理错误: {str(e)}")
        return 1


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


def normalize_url(url):
    """标准化URL，去除多余参数，用于去重比较"""
    try:
        parsed = urlparse(url)
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip('/') if parsed.path else '/',
            '',  # params
            '',  # query
            ''   # fragment
        ))
        return normalized
    except:
        return url


def get_url_hash(url):
    """获取URL的哈希值，用于快速比较"""
    normalized = normalize_url(url)
    return hashlib.md5(normalized.encode('utf-8')).hexdigest()


def is_duplicate_url(url):
    """检查URL是否重复"""
    url_hash = get_url_hash(url)
    with url_cache_lock:
        if url_hash in url_cache:
            return True
        url_cache.add(url_hash)
        return False


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
        existing = set()
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
                existing = set(line.strip() for line in f if line.strip())

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
        ip = ip_address(host)
        return 'ipv6' if isinstance(ip, IPv6Address) else 'ipv4'
    except ValueError:
        return 'ipv4'
    except Exception as e:
        write_log(f"⚠️ IP类型检测异常: {str(e)} ← {url}")
        return 'ipv4'


def get_protocol(url):
    """获取URL的协议类型"""
    try:
        return urlparse(url).scheme.lower()
    except:
        return 'unknown'


def test_https_certificate(domain, port=443):
    """测试HTTPS证书有效性"""
    try:
        context = ssl.create_default_context()
        with socket.create_connection((domain, port), timeout=5) as sock:
            with context.wrap_socket(sock, server_hostname=domain) as ssock:
                cert = ssock.getpeercert()
                not_after = cert.get('notAfter', '')
                if not_after:
                    return True, "证书有效"
                return True, "证书信息获取成功"
    except ssl.SSLError as e:
        return False, f"SSL错误: {str(e)}"
    except Exception as e:
        return False, f"证书检查失败: {str(e)}"


def check_dependencies():
    """检查必要依赖"""
    try:
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
            urls = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]

        print(f"  发现 {len(urls)} 个订阅地址")
        for idx, url in enumerate(urls, 1):
            try:
                print(f"\n🌐 正在获取源 ({idx}/{len(urls)})：{url[:50]}...")
                response = requests.get(url, timeout=15, verify=HTTPS_VERIFY)
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
    """解析M3U格式内容（增强版：提取logo、group、display_name）"""
    channels = []
    current = None

    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue

        if line.startswith('#EXTINF:'):
            if current is not None:
                channels.append(current)

            name_match = re.search(r'tvg-name\s*=\s*"([^"]*)"', line)
            name = name_match.group(1) if name_match else '未知频道'

            logo_match = re.search(r'tvg-logo\s*=\s*"([^"]*)"', line)
            group_match = re.search(r'group-title\s*=\s*"([^"]*)"', line)

            display_name = name
            if ',' in line:
                parts = line.split(',')
                if len(parts) > 1:
                    display_name = parts[-1].strip()

            current = {
                'name': name,
                'display_name': display_name,
                'logo': logo_match.group(1) if logo_match else '',
                'group': group_match.group(1) if group_match else '',
                'urls': []
            }

        elif line and not line.startswith('#'):
            if current is not None:
                url = line.strip()
                # 修复多余斜杠
                if url.startswith('http:///'):
                    url = url.replace('http:///', 'http://')
                current['urls'].append(url)
            else:
                channels.append({
                    'name': '匿名频道',
                    'display_name': '匿名频道',
                    'logo': '',
                    'group': '',
                    'urls': [line.strip()]
                })

        elif line.startswith('#EXTM3U'):
            continue

    if current is not None:
        channels.append(current)

    # 展开多个URL
    result = []
    for channel in channels:
        for url in channel['urls']:
            result.append({
                'name': channel['name'],
                'display_name': channel.get('display_name', channel['name']),
                'logo': channel.get('logo', ''),
                'group': channel.get('group', ''),
                'url': url
            })

    return result


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


def filter_by_template(sources, alias_map):
    """根据配置决定是否只保留模板内的源"""
    if TEST_ALL_SOURCES:
        print(f"\n📋 模式: 检测所有源 (共 {len(sources)} 个)")
        return sources

    if not alias_map:
        print("\n⚠️ 模板为空，回退为检测所有源")
        return sources

    print(f"\n📋 模式: 只检测模板内的源")
    print(f"   模板频道/别名数: {len(alias_map)}")

    filtered = []
    skipped = 0
    for s in sources:
        name = s.get('name', '')
        # 优先匹配标准名，再匹配别名
        if name in alias_map or name in alias_map.values():
            filtered.append(s)
        else:
            skipped += 1

    print(f"✅ 保留 {len(filtered)}/{len(sources)} 个模板内源，跳过 {skipped} 个模板外源")
    return filtered


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
        parsed_url = urlparse(url)
        if not parsed_url.scheme:
            write_log(f"🚫 无效URL格式: {url}")
            continue

        if s.get('whitelist', False):
            filtered.append(s)
            continue

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
            return 100
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


def test_https_specific(url, domain):
    """HTTPS协议特殊检测"""
    try:
        cert_valid, cert_msg = test_https_certificate(domain)

        start_time = time.time()
        with requests.Session() as session:
            response = session.get(url,
                                   stream=True,
                                   timeout=(3.05, SPEED_TEST_DURATION + 2),
                                   allow_redirects=True,
                                   verify=HTTPS_VERIFY,
                                   headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
            response.raise_for_status()

            total_bytes = 0
            data_start = time.time()
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    total_bytes += len(chunk)
                if (time.time() - data_start) >= SPEED_TEST_DURATION:
                    break

            duration = max(time.time() - data_start, 0.001)
            speed = (total_bytes / 1024) / duration

            https_info = f" | 证书: {'有效' if cert_valid else '无效'}"
            log_msg = (f"✅ HTTPS测速成功: {url}\n"
                       f"   速度: {speed:.2f}KB/s | 数据量: {total_bytes / 1024:.1f}KB | "
                       f"总耗时: {time.time() - start_time:.2f}s{https_info}")
            write_log(log_msg)
            return speed

    except requests.exceptions.SSLError as e:
        log_msg = f"❌ HTTPS SSL错误: {url} | 锒误: {str(e)}"
        write_log(log_msg)
        return 0
    except Exception as e:
        domain = get_domain(url)
        update_blacklist(domain)
        log_msg = f"❌ HTTPS测速失败: {url} | 错误: {str(e)}"
        write_log(log_msg)
        return 0


def test_speed(url):
    """增强版测速函数，支持多协议和HTTPS证书检测"""
    try:
        protocol = get_protocol(url)

        # RTMP协议处理
        if protocol in ['rtmp', 'rtmps']:
            return test_rtmp(url)

        # 非常规协议跳过
        if protocol not in ['http', 'https']:
            write_log(f"⚠️ 跳过非常规协议: {url}")
            return 0

        # HTTPS协议特殊处理
        if protocol == 'https':
            domain = get_domain(url)
            if domain:
                return test_https_specific(url, domain)
            else:
                write_log(f"⚠️ 无法提取HTTPS域名: {url}")
                return 0

        # 普通HTTP请求
        start_time = time.time()
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
                if (time.time() - data_start) >= SPEED_TEST_DURATION:
                    break

            duration = max(time.time() - data_start, 0.001)
            speed = (total_bytes / 1024) / duration

            # 速度阈值检查
            if speed > SPEED_THRESHOLD:
                status = "✅ 通过阈值"
            else:
                status = "🚫 未达阈值"

            log_msg = (f"{status} HTTP测速: {url}\n"
                       f"   速度: {speed:.2f}KB/s | 数据量: {total_bytes / 1024:.1f}KB | "
                       f"总耗时: {time.time() - start_time:.2f}s | 阈值: {SPEED_THRESHOLD}KB/s")
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
    """处理所有源并进行测速，应用速度阈值过滤和URL去重"""
    total = len(sources)
    if total == 0:
        print("\n⚠️ 没有可处理的源")
        return []

    print(f"\n🔍 开始检测 {total} 个源 (并发数: {MAX_WORKERS})")
    print(f"📊 速度阈值: {SPEED_THRESHOLD}KB/s")
    processed = []
    processed_count = 0
    passed_count = 0
    duplicate_count = 0

    # 协议统计
    protocol_stats = {}
    seen_urls = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_source = {}
        for s in sources:
            # 提交前检查URL去重
            url_hash = get_url_hash(s['url'])
            if url_hash in seen_urls:
                duplicate_count += 1
                print(f"⏭️  跳过重复URL: {s['name']} | {s['url'][:50]}...")
                continue
            seen_urls.add(url_hash)

            future = executor.submit(
                lambda s: (s['name'], s['url'], test_speed(s['url']),
                           get_ip_type(s['url']), get_protocol(s['url'])), s)
            future_to_source[future] = s

        for future in concurrent.futures.as_completed(future_to_source):
            try:
                name, url, speed, ip_type, protocol = future.result()
                with counter_lock:
                    processed_count += 1
                    progress = f"[{processed_count}/{total}]"

                    if protocol not in protocol_stats:
                        protocol_stats[protocol] = {'total': 0, 'passed': 0}
                    protocol_stats[protocol]['total'] += 1

                speed_str = f"{speed:>7.2f}KB/s"
                name_display = name[:20].ljust(20)
                protocol_icon = "🔒" if protocol == "https" else "📹" if protocol in ['rtmp', 'rtmps'] else "🌐"

                # 应用速度阈值过滤
                if speed > SPEED_THRESHOLD:
                    status = "✅"
                    passed_count += 1
                    protocol_stats[protocol]['passed'] += 1

                    # 再次检查URL去重（多线程环境下）
                    if not is_duplicate_url(url):
                        processed.append((name, url, speed, ip_type, protocol))
                    else:
                        print(f"🔁 检测到重复URL（已跳过）: {name} | {url[:50]}...")
                        passed_count -= 1
                else:
                    status = "❌"

                print(f"{progress} {status} 频道: {name_display} | 速度:{speed_str} | {protocol_icon}{protocol.upper()} | {url[:80]}...")

            except Exception as e:
                source = future_to_source[future]
                error_msg = f"⚠️ 处理异常: {str(e)} ← {source['url'][:50]}..."
                print(error_msg)
                write_log(error_msg)

    # 打印统计信息
    print(f"\n📊 速度阈值过滤结果:")
    print(f"   📡 总检测数: {processed_count}")
    print(f"   🔁 跳过重复: {duplicate_count}")
    print(f"   ✅ 通过数: {passed_count} (速度 > {SPEED_THRESHOLD}KB/s)")
    print(f"   ❌ 淘汰数: {processed_count - passed_count} (速度 ≤ {SPEED_THRESHOLD}KB/s)")
    print(f"   📈 通过率: {passed_count / max(processed_count, 1) * 100:.1f}%")

    print(f"\n📊 协议分布:")
    for protocol, data in protocol_stats.items():
        icon = "🔒" if protocol == "https" else ("📹" if protocol in ['rtmp', 'rtmps'] else "🌐")
        p = data['passed']
        t = data['total']
        print(f"   {icon} {protocol.upper():<6}: {p}/{t} 通过 ({p / max(t, 1) * 100:.1f}%)")

    # 保存黑名单
    save_blacklist()

    print(f"\n✅ 检测完成 - 有效源: {len(processed)}/{total}")
    return processed


def organize_channels(processed, alias_map, group_map):
    """整理频道数据，进行URL去重并按速度排序"""
    print("\n📚 整理频道数据...")
    organized = {'ipv4': OrderedDict(), 'ipv6': OrderedDict()}
    duplicate_stats = {'total': 0, 'channel': {}}

    for name, url, speed, ip_type, protocol in processed:
        if ip_type not in ('ipv4', 'ipv6'):
            ip_type = 'ipv4'

        std_name = alias_map.get(name, name)
        group = group_map.get(std_name, '其他')

        if group not in organized[ip_type]:
            organized[ip_type][group] = OrderedDict()
        if std_name not in organized[ip_type][group]:
            organized[ip_type][group][std_name] = []

        # 检查同一频道下是否有重复URL
        existing_urls = {normalize_url(u[0]) for u in organized[ip_type][group][std_name]}
        normalized_url = normalize_url(url)

        if normalized_url in existing_urls:
            # 找到重复源，保留速度更快的
            for i, (existing_url, existing_speed, existing_protocol) in enumerate(organized[ip_type][group][std_name]):
                if normalize_url(existing_url) == normalized_url:
                    if speed > existing_speed:
                        organized[ip_type][group][std_name][i] = (url, speed, protocol)
                        duplicate_stats['total'] += 1
                        duplicate_stats['channel'][std_name] = duplicate_stats['channel'].get(std_name, 0) + 1
                        print(f"🔄 频道 '{std_name}' 替换重复URL: 新速度 {speed:.1f}KB/s > 旧速度 {existing_speed:.1f}KB/s")
                    break
        else:
            organized[ip_type][group][std_name].append((url, speed, protocol))

    # 打印去重统计
    if duplicate_stats['total'] > 0:
        print(f"🔁 频道内去重: 共清理 {duplicate_stats['total']} 个重复源")

    # 对每个频道的源按速度排序
    for ip_type in ['ipv4', 'ipv6']:
        for group in organized[ip_type]:
            for channel in organized[ip_type][group]:
                organized[ip_type][group][channel].sort(key=lambda x: x[1], reverse=True)

    return organized


def deduplicate_final_output(txt_lines, m3u_lines):
    """对最终输出进行去重"""
    print("\n🔁 对最终输出进行去重...")

    # TXT去重
    txt_dict = {}
    txt_duplicates = 0
    for line in txt_lines:
        if line.endswith(',#genre#'):
            txt_dict[line] = line
        elif ',' in line:
            channel, url = line.split(',', 1)
            key = f"{channel},{normalize_url(url)}"
            if key not in txt_dict:
                txt_dict[key] = line
            else:
                txt_duplicates += 1

    deduped_txt = list(txt_dict.values())

    # M3U去重
    m3u_dict = {}
    m3u_duplicates = 0
    i = 0
    while i < len(m3u_lines):
        if m3u_lines[i].startswith('#EXTINF:'):
            if i + 1 < len(m3u_lines) and not m3u_lines[i + 1].startswith('#'):
                extinf_line = m3u_lines[i]
                url_line = m3u_lines[i + 1]
                key = normalize_url(url_line)
                if key not in m3u_dict:
                    m3u_dict[key] = (extinf_line, url_line)
                else:
                    m3u_duplicates += 1
                i += 2
            else:
                i += 1
        else:
            i += 1

    deduped_m3u = ['#EXTM3U x-tvg-url="https://gh.catmak.name/https://raw.githubusercontent.com/Guovin/iptv-api/refs/heads/master/output/epg/epg.gz"']
    for extinf, url in m3u_dict.values():
        deduped_m3u.append(extinf)
        deduped_m3u.append(url)

    if txt_duplicates > 0 or m3u_duplicates > 0:
        print(f"✅ 去重完成: 移除 {txt_duplicates} 个重复TXT行，{m3u_duplicates} 个重复M3U源")

    return deduped_txt, deduped_m3u


def finalize_output(organized, group_order, channel_order):
    """生成输出文件 - 保持原目录结构，文件名带ipv4/ipv6前缀"""
    print("\n📂 生成结果文件...")

    for ip_type in ['ipv4', 'ipv6']:
        txt_file = os.path.join(OUTPUT_DIR, f'{ip_type}_result.txt')
        m3u_file = os.path.join(OUTPUT_DIR, f'{ip_type}_result.m3u')

        txt_lines = []
        m3u_lines = [
            '#EXTM3U x-tvg-url="https://gh.catmak.name/https://raw.githubusercontent.com/Guovin/iptv-api/refs/heads/master/output/epg/epg.gz"'
        ]

        total_sources = 0
        speed_stats = []
        seen_channels = set()

        # 按模板顺序处理分组
        for group in group_order:
            if group not in organized[ip_type]:
                continue

            txt_lines.append(f"{group},#genre#")

            # 处理模板频道
            for channel in channel_order.get(group, []):
                if channel not in organized[ip_type][group]:
                    continue

                all_urls = organized[ip_type][group][channel]

                # 频道内去重
                seen_in_channel = set()
                unique_urls = []
                for url, speed, protocol in all_urls:
                    normalized_url = normalize_url(url)
                    if normalized_url not in seen_in_channel:
                        seen_in_channel.add(normalized_url)
                        unique_urls.append((url, speed, protocol))

                for url, speed, protocol in unique_urls:
                    txt_lines.append(f"{channel},{url}")
                    total_sources += 1
                    speed_stats.append(speed)

                    protocol_icon = "🔒" if protocol == "https" else "📹" if protocol in ['rtmp', 'rtmps'] else "🌐"
                    m3u_lines.append(f'#EXTINF:-1 tvg-name="{channel}" group-title="{group}",{protocol_icon} {channel} | {speed:.1f}KB/s')
                    m3u_lines.append(url)

                seen_channels.add(channel)

            # 处理额外频道
            extra = sorted(
                [c for c in organized[ip_type][group] if c not in channel_order.get(group, [])],
                key=lambda x: x.lower()
            )
            for channel in extra:
                all_urls = organized[ip_type][group][channel]

                seen_in_channel = set()
                unique_urls = []
                for url, speed, protocol in all_urls:
                    normalized_url = normalize_url(url)
                    if normalized_url not in seen_in_channel:
                        seen_in_channel.add(normalized_url)
                        unique_urls.append((url, speed, protocol))

                if unique_urls:
                    for url, speed, protocol in unique_urls:
                        txt_lines.append(f"{channel},{url}")
                        total_sources += 1
                        speed_stats.append(speed)

                        protocol_icon = "🔒" if protocol == "https" else "📹" if protocol in ['rtmp', 'rtmps'] else "🌐"
                        m3u_lines.append(f'#EXTINF:-1 tvg-name="{channel}" group-title="{group}",{protocol_icon} {channel} | {speed:.1f}KB/s')
                        m3u_lines.append(url)

                    seen_channels.add(channel)

        # 处理"其他"分组
        if '其他' in organized[ip_type]:
            txt_lines.append("其他,#genre#")
            for channel in sorted(organized[ip_type]['其他'].keys(), key=lambda x: x.lower()):
                if channel in seen_channels:
                    continue

                all_urls = organized[ip_type]['其他'][channel]

                seen_in_channel = set()
                unique_urls = []
                for url, speed, protocol in all_urls:
                    normalized_url = normalize_url(url)
                    if normalized_url not in seen_in_channel:
                        seen_in_channel.add(normalized_url)
                        unique_urls.append((url, speed, protocol))

                if unique_urls:
                    for url, speed, protocol in unique_urls:
                        txt_lines.append(f"{channel},{url}")
                        total_sources += 1
                        speed_stats.append(speed)

                        protocol_icon = "🔒" if protocol == "https" else "📹" if protocol in ['rtmp', 'rtmps'] else "🌐"
                        m3u_lines.append(f'#EXTINF:-1 tvg-name="{channel}" group-title="其他",{protocol_icon} {channel} | {speed:.1f}KB/s')
                        m3u_lines.append(url)

        # 最终去重
        txt_lines, m3u_lines = deduplicate_final_output(txt_lines, m3u_lines)

        # 写入文件
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(txt_lines))
        with open(m3u_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(m3u_lines))

        # 统计信息
        if speed_stats:
            avg_speed = sum(speed_stats) / len(speed_stats)
            max_speed = max(speed_stats)
            min_speed = min(speed_stats)
        else:
            avg_speed = max_speed = min_speed = 0

        print(f"✅ 已生成 {ip_type.upper()} 文件:")
        print(f"   📄 {txt_file} - {len(txt_lines)} 行")
        print(f"   📺 {m3u_file} - {len(m3u_lines)} 行")
        print(f"   📊 统计: {total_sources} 个源 | 平均速度: {avg_speed:.1f}KB/s")
        print(f"   📈 速度范围: {min_speed:.1f} - {max_speed:.1f}KB/s")

        # 协议分布统计
        protocol_count = {}
        for group in organized[ip_type]:
            for channel in organized[ip_type][group]:
                for url, speed, protocol in organized[ip_type][group][channel]:
                    protocol_count[protocol] = protocol_count.get(protocol, 0) + 1

        if protocol_count:
            print(f"   🌐 协议分布: {', '.join([f'{p.upper()}:{c}' for p, c in protocol_count.items()])}")


if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("🎬 IPTV直播源处理脚本（增强版）")
    print("=" * 60)

    # 运行次数管理
    run_count = manage_run_count()

    print(f"🔧 配置参数:")
    print(f"   📊 速度阈值: {SPEED_THRESHOLD}KB/s")
    print(f"   🔢 运行次数: {run_count}/{RESET_COUNT}")
    print(f"   🔐 HTTPS证书验证: {'开启' if HTTPS_VERIFY else '关闭'}")
    print(f"   ⏱️  测速时长: {SPEED_TEST_DURATION}秒")
    print(f"   👥 最大并发数: {MAX_WORKERS}")
    print(f"   📁 输出目录: {OUTPUT_DIR}")
    print(f"   📋 模板过滤: {'检测所有源' if TEST_ALL_SOURCES else '只检测模板内的源'}")
    print(f"   🔁 去重功能: 已启用")

    # 检查依赖
    if not check_dependencies():
        exit(1)

    # 初始化日志文件
    with open(SPEED_LOG, 'w', encoding='utf-8') as f:
        f.write(f"测速日志 {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"速度阈值: {SPEED_THRESHOLD}KB/s\n")
        f.write(f"运行次数: {run_count}\n")
        f.write(f"HTTPS验证: {HTTPS_VERIFY}\n\n")

    # 清除URL缓存
    url_cache.clear()

    # 初始化数据
    alias_map, group_map, group_order, channel_order = parse_demo_file()
    sources = fetch_sources() + parse_local()
    blacklist = read_blacklist()

    # 处理流程
    filtered = filter_sources(sources, blacklist)
    filtered = filter_by_template(filtered, alias_map)
    processed = process_sources(filtered)
    organized = organize_channels(processed, alias_map, group_map)
    finalize_output(organized, group_order, channel_order)

    print("\n" + "=" * 60)
    print("🎉 处理完成！")
    print(f"🔢 本次运行次数: {run_count}")
    if run_count >= RESET_COUNT - 1:
        print(f"⚠️ 下次运行将清空黑名单")
    print(f"📁 结果文件:")
    print(f"   {OUTPUT_DIR}/ipv4_result.txt, ipv4_result.m3u")
    print(f"   {OUTPUT_DIR}/ipv6_result.txt, ipv6_result.m3u")
    print("🔍 所有协议源已合并到同一文件中")
    print("✅ 去重功能已启用，已移除重复的URL")
    print("=" * 60)
