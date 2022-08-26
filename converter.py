import argparse
import datetime
import json
import os
import pathlib
from typing import List, Union


class Message:
    def __init__(self, content: dict, token: str = "") -> None:
        self.content = content
        self.ts: str = content["ts"]
        self.timestamp: float = float(self.ts)
        self.date: datetime.datetime = datetime.datetime.fromtimestamp(self.timestamp)
        self.token = token
        self.add_token_to_url()

    def add_reply(self, user: str, ts: str) -> None:
        if not ("replies" in self.content):
            self.content["replies"] = []
        self.content["replies"].append({"user": user, "ts": ts})
        self.content["replies"] = sorted(
            self.content["replies"], key=lambda x: float(x["ts"])
        )

    def add_token_to_url(self) -> None:
        if "files" in self.content and self.token:
            for file in self.content["files"]:
                if "url_private" in file:
                    file["url_private"] += f"?t={self.token}"
                if "url_private_download" in file:
                    file["url_private_download"] += f"?t={self.token}"


class Channel:
    def __init__(self, id: str, name: str) -> None:
        self.id: str = id
        self.name: str = name
        self.messages: List[Message] = []

    def find_message_by_ts(self, ts: str) -> Union[Message, None]:
        for msg in self.messages:
            if msg.ts == ts:
                return msg
        else:
            return None

    def add_message(self, msg: Message):
        if not self.find_message_by_ts(msg.ts):
            self.messages.append(msg)

    def sort_messages(self) -> None:
        self.messages = sorted(self.messages, key=lambda x: x.timestamp)


class Converter:
    def __init__(
        self, input_dir: os.PathLike, output_dir: os.PathLike, token: str = ""
    ) -> None:
        self.input_dir: pathlib.Path = pathlib.Path(input_dir)
        self.output_dir: pathlib.Path = pathlib.Path(output_dir)
        self.channel_list = []
        self.channels: List[Channel] = []
        self.token = token

        with open(self.input_dir.joinpath("user_list.json")) as f:
            self.user_list = json.load(f)
        with open(self.input_dir.joinpath("channel_list.json")) as f:
            channel_list = json.load(f)
            for ch in channel_list:
                if not (ch["is_im"] or ch["is_mpim"]):
                    self.channel_list.append(ch)
                    self.load_ch(ch)

    def load_ch(self, ch: dict):
        ch_id = ch["id"]
        ch_name = ch["name"]
        channel_obj = Channel(ch_id, ch_name)
        ch_msgs_path = self.input_dir.joinpath(f"channel_{ch_id}.json")
        ch_replies_path = self.input_dir.joinpath(f"channel-replies_{ch_id}.json")
        with open(ch_msgs_path) as f:
            ch_msgs = json.load(f)
        with open(ch_replies_path) as f:
            ch_replies = json.load(f)
        for replies in ch_replies:
            for i, reply in enumerate(replies):
                if i == 0:
                    # Root message
                    root_message = Message(reply, token=self.token)
                else:
                    # Replies
                    message = Message(reply, token=self.token)
                    root_message.add_reply(reply["user"], reply["ts"])
                    channel_obj.add_message(message)
            channel_obj.add_message(root_message)
        for msg in ch_msgs:
            channel_obj.add_message(Message(msg, token=self.token))
        channel_obj.sort_messages()
        self.channels.append(channel_obj)

    def save_json(self, data, output_file: os.PathLike):
        with open(pathlib.Path(output_file), mode="w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    def convert(self):
        """
        output_dir/
        ├── users.json
        ├── channels.json
        ├── <channel_name1>
        │     ├── yyyy-MM-dd.json
        │     :
        │     └── yyyy-MM-dd.json
        └── <channel_name2>
        """

        users_json_path = self.output_dir.joinpath("users.json")
        self.save_json(self.user_list, users_json_path)

        channels_json_path = self.output_dir.joinpath("channels.json")
        self.save_json(self.channel_list, channels_json_path)

        for ch in self.channels:
            ch_output_dir = self.output_dir.joinpath(f"{ch.name}")
            ch_output_dir.mkdir()

            ch.sort_messages()

            msgs = []
            current_date = ch.messages[0].date.date()

            for msg in ch.messages:
                if msg.date.date() > current_date:
                    self.save_json(
                        msgs,
                        ch_output_dir.joinpath(
                            f"{current_date.strftime(('%Y-%m-%d'))}.json"
                        ),
                    )
                    current_date = msg.date.date()
                    msgs.clear()
                msgs.append(msg.content)
            else:
                self.save_json(
                    msgs,
                    ch_output_dir.joinpath(
                        f"{current_date.strftime(('%Y-%m-%d'))}.json"
                    ),
                )


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-i", "--input", help="Input directory")
    argparser.add_argument("-o", "--output", help="Output directory")
    argparser.add_argument(
        "--token",
        type=str,
        help="Slack token which is used to download files",
        default="",
    )
    args = argparser.parse_args()

    converter = Converter(
        pathlib.Path(args.input), pathlib.Path(args.output), token=args.token
    )
    converter.convert()
