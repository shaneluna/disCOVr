import trafilatura

# good doc: https://trafilatura.readthedocs.io/en/latest/usage-python.html

if __name__ == '__main__':
    # downloaded = trafilatura.fetch_url('https://github.blog/2019-03-29-leader-spotlight-erin-spiceland/')
    downloaded = trafilatura.fetch_url('https://medium.com/@carusot42/installing-and-loading-spatialite-on-macos-28bf677f0436')
    result = trafilatura.extract(downloaded)
    print(result)