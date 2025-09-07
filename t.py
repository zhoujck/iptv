import requests
from bs4 import BeautifulSoup
import os
import re
from urllib.parse import urljoin

def download_content(url, save_dir):
    """下载指定URL的内容并保存到文件"""
    try:
        # 发送请求获取内容
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # 检查请求是否成功
        
        # 创建保存目录（如果不存在）
        os.makedirs(save_dir, exist_ok=True)
        
        # 生成文件名（从URL提取或使用哈希值）
        filename = re.sub(r'[^\w\-_.]', '_', url.split('/')[-1])
        if not filename:  # 如果文件名是空的，使用简单哈希
            filename = f"page_{hash(url)}.txt"
        if not filename.endswith('.txt'):
            filename += '.txt'
        
        # 保存内容
        file_path = os.path.join(save_dir, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        print(f"成功下载: {url} -> {file_path}")
        return file_path
        
    except Exception as e:
        print(f"下载失败 {url}: {str(e)}")
        return None

def get_spider_links(initial_url):
    """从初始URL获取内容，并提取与'spider'相关的链接"""
    try:
        # 获取初始页面内容
        response = requests.get(initial_url, timeout=10)
        response.raise_for_status()
        
        # 解析HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 提取所有链接
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            # 处理相对链接
            full_url = urljoin(initial_url, href)
            links.append(full_url)
        
        # 筛选包含'spider'的链接（不区分大小写）
        spider_links = [link for link in links if 'spider' in link.lower()]
        
        print(f"找到 {len(spider_links)} 个与'spider'相关的链接")
        return spider_links
        
    except Exception as e:
        print(f"获取初始页面失败: {str(e)}")
        return []

def main(initial_url, save_directory='spider_contents'):
    """主函数：获取spider链接并下载内容"""
    print(f"开始处理初始链接: {initial_url}")
    
    # 获取与spider相关的链接
    spider_links = get_spider_links(initial_url)
    
    if not spider_links:
        print("没有找到与'spider'相关的链接")
        return
    
    # 下载每个链接的内容
    for link in spider_links:
        download_content(link, save_directory)
    
    print("所有操作完成")

if __name__ == "__main__":
    # 在这里替换为你的初始链接
    initial_url = "http://ok321.top/tv"  # 请替换为实际的初始链接
    main(initial_url)
