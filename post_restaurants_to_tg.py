import json
import os
import re
import time
import requests

BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHANNEL = '@restoranvietnam'
PROGRESS_FILE = 'post_progress.json'
DELAY = 3  # seconds between posts


def clean_title(title):
    t = re.sub(r'^[\U0001F300-\U0001FFFF\u2600-\u26FF\u2700-\u27BF\s]+', '', title)
    t = re.sub(r'^РЕСТОРАН:\s*', '', t)
    t = re.sub(r'^НАЗВАНИЕ:\s*', '', t)
    t = re.sub(r'\s*сапфир.*', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\[.*?\]|\(.*?\)', '', t)
    t = re.sub(r'\s{2,}', ' ', t).strip()
    return t


def load_restaurants():
    with open('listings_vietnam.json', encoding='utf-8') as f:
        data = json.load(f)
    result = []
    for item in data['restaurants']:
        if item['title'] == 'Channel created':
            continue
        desc = item.get('description', '')
        if len(desc) < 80:
            continue
        photos = item.get('photos') or item.get('images') or []
        if not photos:
            continue
        result.append({
            'id': item['id'],
            'title': clean_title(item['title']),
            'description': desc,
            'photos': photos[:10],
        })
    return result


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {'posted_ids': []}


def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)


def send_media_group(photos, caption):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMediaGroup'
    media = []
    for i, photo_url in enumerate(photos):
        item = {'type': 'photo', 'media': photo_url}
        if i == 0:
            item['caption'] = caption[:1024]
            item['parse_mode'] = 'HTML'
        media.append(item)
    resp = requests.post(url, json={
        'chat_id': CHANNEL,
        'media': media
    }, timeout=30)
    return resp.json()


def send_single_photo(photo_url, caption):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto'
    resp = requests.post(url, json={
        'chat_id': CHANNEL,
        'photo': photo_url,
        'caption': caption[:1024],
        'parse_mode': 'HTML'
    }, timeout=30)
    return resp.json()


def main():
    if not BOT_TOKEN:
        print('ERROR: TELEGRAM_BOT_TOKEN not set')
        return

    restaurants = load_restaurants()
    progress = load_progress()
    posted_ids = set(progress['posted_ids'])

    to_post = [r for r in restaurants if r['id'] not in posted_ids]
    print(f'Total: {len(restaurants)} | Already posted: {len(posted_ids)} | To post: {len(to_post)}')

    for i, r in enumerate(to_post):
        caption = f"<b>🍽 {r['title']}</b>\n\n{r['description']}"
        photos = r['photos']

        print(f"[{i+1}/{len(to_post)}] Posting: {r['title'][:50]}  ({len(photos)} photos)")

        try:
            if len(photos) == 1:
                result = send_single_photo(photos[0], caption)
            else:
                result = send_media_group(photos, caption)

            if result.get('ok'):
                posted_ids.add(r['id'])
                progress['posted_ids'] = list(posted_ids)
                save_progress(progress)
                print(f"  ✓ OK")
            else:
                err = result.get('description', 'unknown')
                print(f"  ✗ ERROR: {err}")
                # FloodWait handling
                if 'Too Many Requests' in err or 'retry after' in err.lower():
                    wait = int(re.search(r'(\d+)', err).group(1)) + 2 if re.search(r'(\d+)', err) else 30
                    print(f"  FloodWait: sleeping {wait}s...")
                    time.sleep(wait)
                    # Retry once
                    if len(photos) == 1:
                        result = send_single_photo(photos[0], caption)
                    else:
                        result = send_media_group(photos, caption)
                    if result.get('ok'):
                        posted_ids.add(r['id'])
                        progress['posted_ids'] = list(posted_ids)
                        save_progress(progress)
                        print(f"  ✓ OK (retry)")
                    else:
                        print(f"  ✗ FAILED after retry: {result.get('description')}")

        except Exception as e:
            print(f"  ✗ Exception: {e}")

        time.sleep(DELAY)

    print(f"\nDone! Posted {len(posted_ids)} total.")


if __name__ == '__main__':
    main()
