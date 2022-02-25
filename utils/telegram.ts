import fetch from "node-fetch";

function messageTelegram(
    token: string,
    chatId: string,
    appName: string,
    msg: string,
    error = false
  ) {
    let type = error ? "ERROR" : "INFO";
    let networkType = process.env.NETWORK === "devnet" ? "DEV-NET" : "MAIN-NET";
    const formattedMsg = `%5B[${networkType}]%5D %5B${appName}%5D %5B${type}%5D: ${msg}`;
    let text =
      "https://api.telegram.org/bot" +
      token +
      "/sendMessage?chat_id=" +
      chatId +
      "&parse_mode=Markdown&text=" +
      formattedMsg;
    try {
      fetch(text, {
        method: "GET",
        headers: { "Content-Type": "application/json" },
      });
    } catch (e) {}
  }
  

export async function alert(msg: string, error: boolean) {
    console.log(`${msg}`);
    messageTelegram(
      process.env.TELEGRAM_TOKEN,
      process.env.TELEGRAM_CHAT_ID,
      "ZETA-TRADE-INDEXER",
      msg,
      error
    );
  }