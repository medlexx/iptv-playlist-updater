import os
import re
import time
import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from datetime import datetime
import logging
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('iptv_checker.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IPTVFastChecker:
    def __init__(self):
        # ПУТИ ДЛЯ GITHUB ACTIONS (не для локального ПК!)
        self.sources_dir = Path("sources")  # ← Папка с файлами в репозитории
        self.spisok_file = self.sources_dir / "spisok.m3u"  # ← Список каналов
        self.output_playlist = Path("playlist.m3u")
        
        self.max_workers = 200
        self.timeout = 2
        self.session = None
        
        self.working_links = {}
        self.url_pattern = re.compile(r'(https?://[^\s"\']+)')
        self.domain_cache = {}

    async def init_session(self):
        if self.session is None:
            connector = aiohttp.TCPConnector(
                limit=300, 
                ttl_dns_cache=300,
                enable_cleanup_closed=True
            )
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=self.timeout, connect=1)
            )

    async def close_session(self):
        if self.session:
            await self.session.close()

    def read_required_channels(self):
        """Читает список каналов из sources/spisok.m3u"""
        try:
            if not self.spisok_file.exists():
                logger.warning(f"Файл {self.spisok_file} не найден")
                return []
            
            channels = []
            with open(self.spisok_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
                extinf_pattern = re.compile(r'#EXTINF:.*?,(.+?)(?:\n|$)')
                matches = extinf_pattern.findall(content)
                
                if matches:
                    for match in matches:
                        channel_name = match.strip()
                        channel_name = re.sub(r'\[.*?\]', '', channel_name).strip()
                        channel_name = re.sub(r'\{.*?\}', '', channel_name).strip()
                        if channel_name:
                            channels.append(channel_name)
                else:
                    for line in content.split('\n'):
                        line = line.strip()
                        if line and not line.startswith('#'):
                            if 'http' not in line:
                                channels.append(line)
            
            logger.info(f"Загружено {len(channels)} каналов из spisok.m3u")
            if channels:
                logger.info(f"Примеры каналов: {channels[:5]}")
            return channels
        except Exception as e:
            logger.error(f"Ошибка чтения spisok.m3u: {e}")
            return []

    async def extract_urls_from_file(self, file_path):
        """Извлекает ссылки из файла"""
        urls = []
        try:
            if file_path.name.lower() == 'spisok.m3u':
                return urls
            
            async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = await f.read()
            
            found_urls = self.url_pattern.findall(content)
            
            if not found_urls:
                return urls
            
            for url in found_urls:
                name = "Unknown"
                if '#EXTINF' in content:
                    lines = content.split('\n')
                    for i, line in enumerate(lines):
                        if url in line and i > 0 and '#EXTINF' in lines[i-1]:
                            match = re.search(r'#EXTINF:.*?,(.+?)(?:\n|$)', lines[i-1])
                            if match:
                                name = match.group(1).strip()
                                name = re.sub(r'\[.*?\]|{.*?}', '', name).strip()
                            break
                
                urls.append({'url': url, 'name': name})
            
            if urls:
                logger.info(f"Из {file_path.name}: {len(urls)} ссылок")
            return urls
        except Exception as e:
            logger.error(f"Ошибка {file_path}: {e}")
            return []

    async def collect_all_urls(self):
        """Собирает все ссылки из папки sources"""
        all_urls = []
        
        if not self.sources_dir.exists():
            logger.error(f"Папка {self.sources_dir} не найдена")
            return all_urls
        
        files = list(self.sources_dir.glob("*"))
        logger.info(f"Найдено {len(files)} файлов в папке sources")
        
        tasks = [self.extract_urls_from_file(f) for f in files if f.is_file()]
        results = await asyncio.gather(*tasks)
        
        for result in results:
            all_urls.extend(result)
        
        seen = set()
        unique = []
        for item in all_urls:
            if item['url'] not in seen:
                seen.add(item['url'])
                unique.append(item)
        
        logger.info(f"Всего собрано {len(unique)} уникальных ссылок")
        return unique

    async def quick_check(self, url):
        try:
            domain = url.split('/')[2] if '://' in url else url
            if domain in self.domain_cache:
                return self.domain_cache[domain]
            
            await self.init_session()
            
            async with self.session.head(url, allow_redirects=True) as resp:
                if resp.status in [200, 206, 302, 301, 307, 308]:
                    content_type = resp.headers.get('Content-Type', '').lower()
                    is_video = any(x in content_type for x in ['video', 'mpegurl', 'octet-stream'])
                    self.domain_cache[domain] = is_video
                    return is_video
            
            self.domain_cache[domain] = False
            return False
        except:
            self.domain_cache[domain] = False
            return False

    async def check_all_links(self, urls):
        logger.info(f"Начинаем проверку {len(urls)} ссылок...")
        
        total = len(urls)
        checked = 0
        found = 0
        
        for i in range(0, total, 200):
            batch = urls[i:i+200]
            tasks = [self.quick_check(item['url']) for item in batch]
            results = await asyncio.gather(*tasks)
            
            for j, is_working in enumerate(results):
                checked += 1
                if is_working:
                    url_info = batch[j]
                    self.working_links[url_info['url']] = url_info['name']
                    found += 1
            
            if checked % 5000 == 0:
                logger.info(f"Проверено {checked}/{total} | Найдено: {found}")
        
        logger.info(f"Найдено {len(self.working_links)} рабочих ссылок")
        return self.working_links

    def match_channels(self, required_channels):
        if not required_channels:
            result = defaultdict(list)
            for url, name in self.working_links.items():
                result[name].append(url)
            return {k: v[:10] for k, v in result.items()}
        
        channel_index = {}
        for channel in required_channels:
            channel_lower = channel.lower()
            channel_clean = re.sub(r'[^\w\s]', '', channel_lower)
            channel_index[channel] = (channel_lower, channel_clean)
        
        result = defaultdict(list)
        
        for url, name in self.working_links.items():
            name_lower = name.lower()
            name_clean = re.sub(r'[^\w\s]', '', name_lower)
            
            for channel, (ch_lower, ch_clean) in channel_index.items():
                if (ch_lower in name_lower or 
                    name_lower in ch_lower or
                    ch_clean in name_clean or
                    name_clean in ch_clean):
                    result[channel].append(url)
                    break
        
        final = {}
        for channel, urls in result.items():
            if urls:
                unique_urls = list(dict.fromkeys(urls))[:10]
                final[channel] = unique_urls
        
        logger.info(f"Найдено {len(final)} каналов из списка")
        return final

    async def create_playlist(self, channels):
        if not channels:
            logger.error("Нет каналов для создания плейлиста")
            return False
        
        async with aiofiles.open(self.output_playlist, 'w', encoding='utf-8') as f:
            await f.write('#EXTM3U\n')
            await f.write(f'# Создан: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
            await f.write(f'# Каналов: {len(channels)}\n\n')
            
            for name, urls in channels.items():
                await f.write(f'#EXTINF:-1,{name} ({len(urls)} источников)\n')
                for url in urls:
                    await f.write(f'{url}\n')
                await f.write('\n')
        
        logger.info(f"Создан плейлист {self.output_playlist} с {len(channels)} каналами")
        return True

    async def run(self):
        logger.info("="*60)
        logger.info("ЗАПУСК СУПЕР-БЫСТРОГО ПОИСКА")
        logger.info("="*60)
        
        start = time.time()
        
        required = self.read_required_channels()
        if required:
            logger.info(f"Нужно найти {len(required)} каналов")
        else:
            logger.warning("Список каналов пуст или не найден")
            return
        
        all_urls = await self.collect_all_urls()
        if not all_urls:
            logger.error("Ссылки не найдены")
            return
        
        await self.check_all_links(all_urls)
        
        if not self.working_links:
            logger.error("Рабочих ссылок не найдено")
            return
        
        channels = self.match_channels(required)
        
        if not channels:
            logger.error("Не найдено совпадений с каналами из списка")
            sample = list(self.working_links.values())[:10]
            logger.info(f"Примеры найденных каналов: {sample}")
            return
        
        await self.create_playlist(channels)
        
        elapsed = time.time() - start
        logger.info(f"Готово за {elapsed:.2f} секунд")

async def main():
    checker = IPTVFastChecker()
    try:
        await checker.run()
    finally:
        await checker.close_session()

if __name__ == "__main__":
    asyncio.run(main())
