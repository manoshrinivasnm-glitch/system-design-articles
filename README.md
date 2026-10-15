# System Design Articles

Interview-ready high-level designs for 29 system design problems (21 course problems plus 8 common SDE-2 designs), written to be **talked through in a 30-minute HLD round**.
Every article opens with the final architecture diagram, then walks the same script:
requirements → numbers → API and data model → a simple v1 → where v1 breaks → the scalable v2 (and v3) → the key flow → deep dives → trade-offs → follow-up questions → a 60-second recap.

Site: https://manoshrinivasnm-glitch.github.io/system-design-articles/

New here? Read [How to Run a 30-Minute HLD Interview](p/00-interview-framework.md) first. It is the template every article follows, plus a cheat sheet of the building blocks they reuse.

## System Design Course (21 problems)

<!-- course:start -->
| # | Problem | Level | Read |
|---|---|---|---|
| 0 | How to Run a 30-Minute HLD Interview (Start here) | Guide | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/00-interview-framework.html) · [markdown](p/00-interview-framework.md) |
| 1 | Design a URL Shortener (TinyURL, Bitly) | Easy | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/01-url-shortener.html) · [markdown](p/01-url-shortener.md) |
| 2 | Design a Movie Ticket Booking System (BookMyShow, District, Ticketmaster) | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/02-ticket-booking.html) · [markdown](p/02-ticket-booking.md) |
| 3 | Design an E-commerce Platform (Amazon, Flipkart) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/03-e-commerce.html) · [markdown](p/03-e-commerce.md) |
| 4 | Design a Video Streaming Platform (Netflix, Prime Video, Hotstar) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/04-video-streaming.html) · [markdown](p/04-video-streaming.md) |
| 5 | Design a Hotel Booking App (Airbnb, Booking.com) | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/05-hotel-booking.html) · [markdown](p/05-hotel-booking.md) |
| 6 | Design a Food Delivery App (Zomato, Swiggy, Uber Eats) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/06-food-delivery.html) · [markdown](p/06-food-delivery.md) |
| 7 | Design Proximity Search (Nearby places, Yelp) | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/07-proximity-search.html) · [markdown](p/07-proximity-search.md) |
| 8 | Design a Chat Application (WhatsApp, Messenger) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/08-chat-app.html) · [markdown](p/08-chat-app.md) |
| 9 | Design a Distributed Job Scheduler (Airflow, Temporal, Celery) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/09-job-scheduler.html) · [markdown](p/09-job-scheduler.md) |
| 10 | Design a Collaborative Text Editor (Google Docs, Notion) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/10-collaborative-editor.html) · [markdown](p/10-collaborative-editor.md) |
| 11 | Design a Ride-Sharing App (Uber, Ola, Rapido, Lyft) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/11-ride-sharing.html) · [markdown](p/11-ride-sharing.md) |
| 12 | Design a Stock Broker Trading App (Zerodha, Groww, Upstox) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/12-stock-broker.html) · [markdown](p/12-stock-broker.md) |
| 13 | Design a Social Media Feed (Facebook, Instagram, X) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/13-news-feed.html) · [markdown](p/13-news-feed.md) |
| 14 | Design a Top-K Leaderboard (Game ranks, trending songs) | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/14-leaderboard.html) · [markdown](p/14-leaderboard.md) |
| 15 | Design a Notification System (SMS, OTP, email, push) | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/15-notification-system.html) · [markdown](p/15-notification-system.md) |
| 16 | Design a Distributed Logging System (Splunk, ELK, Loki, Datadog) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/16-distributed-logging.html) · [markdown](p/16-distributed-logging.md) |
| 17 | Design a Payment Gateway (Stripe, PayPal, Razorpay) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/17-payment-gateway.html) · [markdown](p/17-payment-gateway.md) |
| 18 | Design a Cloud Storage Platform (Google Drive, Dropbox) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/18-cloud-storage.html) · [markdown](p/18-cloud-storage.md) |
| 19 | Design an Online Learning Platform (Udemy, Coursera) | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/19-online-learning.html) · [markdown](p/19-online-learning.md) |
| 20 | Design an Email Service (Gmail, Outlook) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/20-email-system.html) · [markdown](p/20-email-system.md) |
| 21 | Design a Distributed Rate Limiter (API throttling) | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/21-rate-limiter.html) · [markdown](p/21-rate-limiter.md) |
<!-- course:end -->

## Common SDE-2 Designs (ChatGPT suggested)

The designs asked most often in SDE-2 rounds. The ones the course already covers link to the same article.

<!-- sde2:start -->
| # | Problem | Level | Read |
|---|---|---|---|
| 1 | Design a URL Shortener (TinyURL, Bitly) · in the course | Easy | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/01-url-shortener.html) · [markdown](p/01-url-shortener.md) |
| 21 | Design a Distributed Rate Limiter (API throttling) · in the course | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/21-rate-limiter.html) · [markdown](p/21-rate-limiter.md) |
| 15 | Design a Notification System (SMS, OTP, email, push) · in the course | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/15-notification-system.html) · [markdown](p/15-notification-system.md) |
| 22 | Design a Distributed Cache (Redis, Memcached) | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/22-distributed-cache.html) · [markdown](p/22-distributed-cache.md) |
| 23 | Design a Unique ID Generator (Twitter Snowflake) | Easy | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/23-unique-id-generator.html) · [markdown](p/23-unique-id-generator.md) |
| 24 | Design a Distributed Key-Value Store (DynamoDB, Cassandra) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/24-key-value-store.html) · [markdown](p/24-key-value-store.md) |
| 25 | Design a Distributed Message Queue (Kafka, RabbitMQ, SQS) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/25-message-queue.html) · [markdown](p/25-message-queue.md) |
| 26 | Design a Web Crawler (Googlebot) | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/26-web-crawler.html) · [markdown](p/26-web-crawler.md) |
| 27 | Design Search Autocomplete (Google typeahead) | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/27-search-autocomplete.html) · [markdown](p/27-search-autocomplete.md) |
| 28 | Design a Metrics and Alerting System (Prometheus, Datadog, Grafana) | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/28-metrics-monitoring.html) · [markdown](p/28-metrics-monitoring.md) |
| 29 | Design Pastebin (Pastebin, GitHub Gist) | Easy | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/29-pastebin.html) · [markdown](p/29-pastebin.md) |
| 8 | Design a Chat Application (WhatsApp, Messenger) · in the course | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/08-chat-app.html) · [markdown](p/08-chat-app.md) |
| 13 | Design a Social Media Feed (Facebook, Instagram, X) · in the course | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/13-news-feed.html) · [markdown](p/13-news-feed.md) |
| 14 | Design a Top-K Leaderboard (Game ranks, trending songs) · in the course | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/14-leaderboard.html) · [markdown](p/14-leaderboard.md) |
| 9 | Design a Distributed Job Scheduler (Airflow, Temporal, Celery) · in the course | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/09-job-scheduler.html) · [markdown](p/09-job-scheduler.md) |
| 18 | Design a Cloud Storage Platform (Google Drive, Dropbox) · in the course | Hard | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/18-cloud-storage.html) · [markdown](p/18-cloud-storage.md) |
| 7 | Design Proximity Search (Nearby places, Yelp) · in the course | Medium | [site](https://manoshrinivasnm-glitch.github.io/system-design-articles/p/07-proximity-search.html) · [markdown](p/07-proximity-search.md) |
<!-- sde2:end -->

## How the pieces fit

| Path | What it is |
|---|---|
| `p/<slug>.md` | One article per problem. Rendered at `/p/<slug>.html`; also readable directly on GitHub, diagrams included. |
| `_data/problems.json` | Master list: title, also-known-as, level and key ideas for each problem. |
| `tools/build_site.py` | Regenerates `index.md`, the table above and the prev/next links. Run before committing. |
| `tools/check_diagrams.mjs` | Parses every Mermaid diagram and rejects Liquid braces. Also runs in GitHub Actions on every push. |
| `ARTICLE_GUIDE.md` | The structure and writing standard every article follows. |

Diagrams are Mermaid code blocks: github.com draws them natively, and the site draws them with `assets/article.js`.

## Verify locally (optional)

Needs Node 20+:

```
npm install
npm run check                                     # every article
node tools/check_diagrams.mjs p/01-url-shortener.md   # one article
```

All text and diagrams are original, written for personal interview prep.
