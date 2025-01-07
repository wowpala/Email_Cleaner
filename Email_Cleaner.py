import imaplib
import email
from email.utils import parsedate_to_datetime, getaddresses
from email.header import decode_header
from datetime import datetime, timezone, timedelta
import configparser
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Set, Tuple, Optional
import sys
from queue import Queue
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import random
import string

# 增加IMAP缓冲区大小以支持批量操作
imaplib._MAXLINE = 10000000


@dataclass
class EmailConfig:
    email_user: str
    email_pass: str
    imap_server: str
    imap_port: int
    target_email: str
    exclude_emails: List[str]
    days_before: int
    thread_count: int
    batch_size: int = 100  # 批量处理的邮件数量


class IMAPConnectionPool:
    def __init__(self, config: EmailConfig, pool_size: int):
        self.config = config
        self.pool = Queue(maxsize=pool_size)
        self.lock = threading.Lock()

        # 初始化连接池
        for _ in range(pool_size):
            connection = self._create_connection()
            if connection:
                self.pool.put(connection)

    def _create_connection(self) -> Optional[imaplib.IMAP4_SSL]:
        try:
            mail = imaplib.IMAP4_SSL(self.config.imap_server, self.config.imap_port)
            mail.login(self.config.email_user, self.config.email_pass)
            mail.select("inbox")
            return mail
        except Exception as e:
            logging.error(f"创建IMAP连接失败: {e}")
            return None

    @contextmanager
    def get_connection(self):
        connection = self.pool.get()
        try:
            yield connection
        finally:
            # 检查连接是否仍然有效，如果无效则创建新连接
            try:
                connection.noop()
                self.pool.put(connection)
            except:
                new_conn = self._create_connection()
                if new_conn:
                    self.pool.put(new_conn)


class EmailCleaner:
    def __init__(self, config_path: str = "config.txt"):
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.local_timezone = datetime.now(timezone.utc).astimezone().tzinfo
        self.days_before_date = datetime.now(self.local_timezone) - timedelta(
            days=self.config.days_before
        )
        self.connection_pool = IMAPConnectionPool(self.config, self.config.thread_count)

    def _load_config(self, config_path: str) -> EmailConfig:
        """加载配置文件并返回配置对象"""
        config_parser = configparser.ConfigParser()
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config_parser.read_file(f)

            return EmailConfig(
                email_user=config_parser.get("credentials", "email_user"),
                email_pass=config_parser.get("credentials", "email_pass"),
                imap_server=config_parser.get("server", "imap_server"),
                imap_port=config_parser.getint("server", "imap_port", fallback=993),
                target_email=config_parser.get("filters", "target_email"),
                exclude_emails=[
                    email.strip()
                    for email in config_parser.get("filters", "exclude_email").split(
                        ","
                    )
                ],
                days_before=config_parser.getint(
                    "filters", "days_before", fallback=365
                ),
                thread_count=config_parser.getint(
                    "performance", "thread_count", fallback=4
                ),
                batch_size=config_parser.getint(
                    "performance", "batch_size", fallback=100
                ),
            )
        except Exception as e:
            print(f"配置文件错误: {e}")
            sys.exit(1)

    def _setup_logging(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger("EmailCleaner")
        logger.setLevel(logging.INFO)

        fh = logging.FileHandler("email_cleaner.log", encoding="utf-8")
        ch = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(threadName)s - %(levelname)s - %(message)s"
        )

        for handler in [fh, ch]:
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    @staticmethod
    def decode_header_value(header: Optional[str]) -> str:
        """解码邮件头部信息"""
        if not header:
            return ""
        try:
            decoded_parts = decode_header(header)
            return "".join(
                (
                    part.decode(charset or "utf-8", errors="ignore")
                    if isinstance(part, bytes)
                    else str(part)
                )
                for part, charset in decoded_parts
            )
        except Exception as e:
            logging.error(f"解码邮件头部时出错: {e}")
            return header

    def process_email_batch(self, uids: List[bytes]) -> List[Dict]:
        """批量处理邮件信息"""
        results = []
        with self.connection_pool.get_connection() as mail:
            # 构建批量获取命令
            uid_list = b",".join(uids)
            status, messages = mail.uid("fetch", uid_list, "(BODY.PEEK[HEADER])")

            if status != "OK":
                self.logger.error(f"批量获取邮件头失败: {status}")
                return results

            # 处理返回的邮件头信息
            for i in range(0, len(messages), 2):
                if not messages[i]:
                    continue

                try:
                    msg = email.message_from_bytes(messages[i][1])
                    uid = messages[i][0].split()[2]  # 获取UID

                    from_ = self.decode_header_value(msg.get("From", ""))
                    to_ = self.decode_header_value(msg.get("To", ""))
                    date_str = msg.get("Date")

                    if date_str:
                        date_time = parsedate_to_datetime(date_str)
                        if date_time.tzinfo is None:
                            date_time = date_time.replace(
                                tzinfo=timezone.utc
                            ).astimezone(self.local_timezone)
                    else:
                        date_time = None

                    to_addresses = [email for name, email in getaddresses([to_])]

                    results.append(
                        {
                            "uid": uid,
                            "from": from_,
                            "to": to_addresses,
                            "date": date_time,
                        }
                    )
                except Exception as e:
                    self.logger.error(f"处理邮件时出错: {e}")
                    continue

        return results

    def search_emails(self) -> List[bytes]:
        """搜索符合条件的邮件"""
        with self.connection_pool.get_connection() as mail:

            # 生成随机字符串
            random_email = (
                "".join(random.choices(string.ascii_lowercase, k=10)) + "@random.str"
            )

            search_criteria = [
                f'TO "{self.config.target_email}"',
                *[f'NOT FROM "{email}"' for email in self.config.exclude_emails],
                f'NOT FROM "{random_email}"',  # 加入随机生成的字符串
                f"BEFORE {self.days_before_date.strftime('%d-%b-%Y')}",
            ]
            search_query = " ".join(search_criteria)
            print("搜索条件 = " + search_query)

            # 搜索符合条件的邮件 UID
            status, messages = mail.uid("search", None, search_query)
            if status != "OK":
                self.logger.error(f"搜索邮件失败: {search_query}")
                return []

            # 获取所有邮件的 UID
            all_uids = messages[0].split()
            self.logger.info(f"找到 {len(all_uids)} 封符合条件的邮件")

            #            print(messages)

            return all_uids

    def get_all_emails_info(self, mail_uids: List[bytes]) -> List[Dict]:
        """并行获取所有邮件信息"""
        # self.logger.info(f"开始获取 {len(mail_uids)} 封邮件的信息...")

        # 将邮件列表分成批次
        batches = [
            mail_uids[i : i + self.config.batch_size]
            for i in range(0, len(mail_uids), self.config.batch_size)
        ]

        all_results = []
        with ThreadPoolExecutor(max_workers=self.config.thread_count) as executor:
            # 并行处理每个批次
            future_to_batch = {
                executor.submit(self.process_email_batch, batch): batch
                for batch in batches
            }

            for future in concurrent.futures.as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    results = future.result()
                    all_results.extend(results)
                    # self.logger.info(f"完成处理 {len(batch)} 封邮件")
                except Exception as e:
                    self.logger.error(f"处理批次时出错: {e}")

        return all_results

    def delete_emails(self, mail_uids: List[bytes]) -> Tuple[int, List[bytes]]:
        """批量删除邮件"""
        deleted_count = 0
        deleted_uids = []

        # 将邮件分批处理
        batches = [
            mail_uids[i : i + self.config.batch_size]
            for i in range(0, len(mail_uids), self.config.batch_size)
        ]

        with self.connection_pool.get_connection() as mail:
            for batch in batches:
                try:
                    uid_list = b",".join(batch)
                    status, _ = mail.uid("store", uid_list, "+FLAGS", "\\Deleted")
                    if status == "OK":
                        deleted_count += len(batch)
                        deleted_uids.extend(batch)
                        # self.logger.info(f"标记删除 {len(batch)} 封邮件")
                except Exception as e:
                    self.logger.error(f"删除邮件批次时出错: {e}")

            # 永久删除标记的邮件
            status, _ = mail.expunge()
            if status != "OK":
                self.logger.error("永久删除标记的邮件失败")

        return deleted_count, deleted_uids

    def write_delete_log(self, emails_info: List[dict]) -> None:
        """写入删除日志"""
        log_path = Path("delete_log.txt")
        try:
            with log_path.open("w", encoding="utf-8") as log:
                log.write("预计删除的邮件列表：\n")
                for email_info in emails_info:
                    if email_info["date"]:
                        log.write(
                            f"邮件 UID: {email_info['uid']}, "
                            f"发件人: {email_info['from']}, "
                            f"收件人: {', '.join(email_info['to'])}, "
                            f"时间: {email_info['date']}\n"
                        )
        except Exception as e:
            self.logger.error(f"写入日志文件失败: {e}")

    def get_total_email_count(self) -> int:
        """获取当前邮箱中的总邮件数量"""
        with self.connection_pool.get_connection() as mail:
            status, response = mail.status("inbox", "(MESSAGES)")
            if status == "OK":
                return int(response[0].split()[2].strip(b")"))
            else:
                self.logger.error("获取邮件总数失败")
                return 0

    def run(self):
        """运行邮件清理程序"""
        try:
            while True:
                # 获取删除前的总邮件数量
                total_before = self.get_total_email_count()
                self.logger.info(f"当前邮件总数量: {total_before}")

                # 搜索符合条件的邮件
                mail_uids = self.search_emails()
                if not mail_uids:
                    self.logger.info("没有找到符合条件的邮件，程序结束")
                    break

                # 批量获取邮件信息
                # self.logger.info("开始获取邮件信息...")
                emails_info = self.get_all_emails_info(mail_uids)
                # self.logger.info(f"成功获取 {len(emails_info)} 封邮件的信息")

                # 生成日志
                self.write_delete_log(emails_info)

                # 打开日志文件供用户确认
                try:
                    import os

                    os.startfile("delete_log.txt")
                except AttributeError:
                    self.logger.info("请手动打开 delete_log.txt 文件查看待删除邮件列表")

                # 用户确认
                if (
                    input("请查看delete_log.txt，是否确认删除以上邮件？(y/n): ")
                    .strip()
                    .lower()
                    == "y"
                ):
                    deleted_count, deleted_uids = self.delete_emails(mail_uids)
                    self.logger.info(f"成功删除 {deleted_count} 封邮件")

                else:
                    print("操作已取消，程序终止。")
                    exit()

                    # 获取删除后的总邮件数量
                    total_after = self.get_total_email_count()
                    self.logger.info(f"删除后的总邮件数量: {total_after}")

        except Exception as e:
            self.logger.error(f"程序执行出错: {e}")


if __name__ == "__main__":
    cleaner = EmailCleaner()
    cleaner.run()
