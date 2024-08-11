from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import os

def initialize_driver():
    options = webdriver.FirefoxOptions()
    options.add_argument('--headless')  # Run in headless mode
    driver = webdriver.Firefox()
    return driver

def perform_search(driver, query):
    driver.get("https://www.lazada.vn")
    time.sleep(10)  # Allow the page to load
    
    search_box = driver.find_element(By.NAME, "q")
    search_box.send_keys(query)
    search_box.send_keys(Keys.RETURN)
    
    # Wait for the search results to load
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".RfADt"))
        )
    except Exception as e:
        print("An error occurred while waiting for search results:", e)
        driver.quit()
        return None

    return driver

def extract_product_data(driver):
    products = []

    # Extract product names and prices
    try:
        boxs = driver.find_elements(By.CSS_SELECTOR, ".Bm3ON")
        
        for box in boxs:
            info = {}
            try:
                name = box.find_element(By.CSS_SELECTOR, ".RfADt").text
            except:
                print("Name er")
                name = None
            try:
                price = int(box.find_element(By.CSS_SELECTOR, ".ooOxS").text.split("₫")[1].replace(",", ""))
            except:
                price = None

            try:
                discount = box.find_element(By.CSS_SELECTOR, ".WNoq3").text.split("%")[0]
            except:
                discount = 0

            try:
                full_stars = box.find_elements(By.CSS_SELECTOR, "i._9-ogB.Dy1nx")
                half_stars = box.find_elements(By.CSS_SELECTOR, "i._9-ogB._4s-Xt")
                star = len(full_stars) + 0.5 * len(half_stars)
            except:
                star = None
            try:
                url = box.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
            except:
                url = None
            try:
                n_fb = int(box.find_elements(By.CSS_SELECTOR, ".qzqFw")[0].text.split("(")[1].split(")")[0])
            except:
                n_fb = None

            # if url:
            #     more_info = extract_additional_info(driver, url)
            #     info.update(more_info)

            info = {"name": name,
                "price": price,
                "discount": discount,
                "star": star,
                "url": url,
                "n_fb": n_fb}
            products.append(info)
    
        
    except Exception as e:
        print("An error occurred while extracting product data:", e)

    return products

def extract_additional_info(driver, url):
    more_info = {}
    original_window = driver.current_window_handle
    print("Go to new window")
    try:
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(url)

        # Wait for the product detail page to load
        WebDriverWait(driver, 10)
        print("Wait done")
        # Extract additional information (example)
        try:
            description = driver.find_element(By.CSS_SELECTOR, ".pdp-product-desc").text
            more_info['description'] = description
        except Exception as e:
            logging.error(f"Error extracting description: {e}")
            more_info['description'] = None

        # Close the new tab and switch back to the original window
        driver.close()
        driver.switch_to.window(original_window)
        print("Back to original window")
    except Exception as e:
        logging.error(f"An error occurred while extracting additional information: {e}")
        driver.close()
        driver.switch_to.window(original_window)
    
    return more_info


def go_to_next_page(driver):
    try:
        # Scroll to the bottom to make sure the next button is visible
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Wait for any lazy loading

        # Check if the next button is enabled
        next_button_li = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "li.ant-pagination-next"))
        )
        if next_button_li.get_attribute("aria-disabled") == "false":
            next_button = next_button_li.find_element(By.CSS_SELECTOR, "button.ant-pagination-item-link")
            next_button.click()
            return True
        else:
            logging.info("Next button is disabled.")
            return False
    except Exception as e:
        logging.info("No more pages or unable to locate the next button.")
        return False



if __name__ == "__main__":
    driver = initialize_driver()
    keyword = "tai nghe"
    driver = perform_search(driver, keyword)
    all_products = []
    count = 0
    while True:
        file_name = f"{keyword}_{count}.json"
        if file_name in os.listdir("data"):
            count +=1
        else:
            break
    # breakpoint()
    if driver:
        while True:
            products = extract_product_data(driver)
            all_products.extend(products)
            with open(f"data/{file_name}","w") as f:
                json.dump(all_products, f,indent=4)
            if not go_to_next_page(driver):
                break
            
            # Wait for the next page to load
            time.sleep(10)  # You might need to adjust the sleep time based on the page load speed
            print("Next page")
        driver.quit()
