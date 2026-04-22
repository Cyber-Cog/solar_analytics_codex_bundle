const { chromium } = require('playwright');
(async () => {
  try {
    const browser = await chromium.launch();
    const page = await browser.newPage();
    await page.goto('http://127.0.0.1:8080/');
    await page.waitForTimeout(1000);
    await page.fill('input[type="email"]', 'admin@solar.com');
    await page.fill('input[type="password"]', 'admin123');
    await page.click('button[type="submit"]');
    await page.waitForTimeout(3000);
    await page.screenshot({ path: 'login_result.png' });
    await browser.close();
    console.log('Login test done!');
  } catch (e) {
    console.error('Test error:', e.message);
    process.exit(1);
  }
})();
