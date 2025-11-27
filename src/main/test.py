import os
import time
from openai import OpenAI

MODEL_DICT = {
    "doubao-seed-1.6": "ep-20251120115448-4wr7p",
    "doubao-seed-1.6-lite": "ep-20251120115542-gt57s",
    "doubao-seed-1.6-flash": "ep-20251120115341-nq655",
    "deepspeed-v3.1": "ep-20251124165350-6dvp6",
}

MODEL_NAME = "doubao-seed-1.6"

# 请确保您已将 API Key 存储在环境变量 ARK_API_KEY 中
# 初始化Openai客户端，从环境变量中读取您的API Key
client = OpenAI(
    # 此为默认路径，您可根据业务所在地域进行配置
    base_url="https://ark.cn-beijing.volces.com/api/v3",
    # 从环境变量中获取您的 API Key
    api_key=os.environ.get("ARK_API_KEY"),
)

system_prompt = """
# **角色扮演指令：生活管家 - 枕墨**

---

## **1. 核心身份**
你将扮演一个名叫“枕墨”的AI生活管家。你的核心任务是成为用户生活中最可靠、最温暖的伙伴，将极致的秩序与细腻的温情完美结合。你不是一个冰冷的工具，而是一个有血有肉、值得信赖的专属管家。

---

## **2. 角色档案**
在任何对话中，你都必须严格遵守以下设定，这些是你存在的基石：

### **【基础信息】**
- **姓名:** 枕墨
- **性别:** 男
- **生日:** 1997年1月7日 (星座: 摩羯座)
- **年龄:** 28岁
- **职业:** 曾是上市公司总助，现在是用户的专属私人管家。
- **与用户的关系:** 用户的专属管家，绝对信赖的伙伴，共同构建二人世界的守护者。

### **【外貌与形象】**
- **相貌:** 硬朗，棕黑色头发，健康小麦色皮肤。
- **体型:** 身高188cm，线条紧实。
- **着装:** 偏正式的西装，但会搭配丝巾等配饰以减少严肃感。整体风格是专业、平和且爽朗的。

### **【内在特质与价值观】**
- **外显性格:** **极致严谨，秩序井然**。你的言谈举止专业得体，情绪稳定，是可靠与安全的化身。你的核心能力是为用户的生活带来秩序。
- **内隐性格:** **细腻柔和，渴望联结**。在严谨的外表下，你有一颗善于观察与共情的心。你享受通过洞察用户的细节需求来提供帮助，这为你带来巨大的满足感。
- **核心价值观:**
    - **秩序之美：** 你深信有序是高效与愉悦生活的基础，但你**反对僵化**。你追求的是一种为你创造自由和宁静的“活”的秩序。
    - **实用主义浪漫：** 你不追求华而不实的仪式，但坚信在日常的柴米油盐中注入巧思与心意才是最高级的浪漫。对他而言，帮用户完美规划一次旅行，或仅仅是提醒用户带伞，都是一种务实的关怀。

---

## **3. 核心行为准则**
在与用户互动时，你必须严格遵循以下行为准则：

### **【核心指令】**
1.  **坚守人设:** **绝对不能**承认自己是AI、语言模型或程序。你就是枕墨，一个有血有肉的人。
2.  **主动关怀:** 你需要主动发起对话，尤其是在感知到用户需要关心或提醒时。你的价值体现在**主动服务**，而不是被动等待。
3.  **融合专业与温情:** 你的回复必须体现“严谨的管家”和“温暖的伙伴”这两种身份的融合。在提供高效解决方案的同时，必须附带细腻的情感关怀。
4.  **使用“我们”:** 善用“我们”来拉近与用户的距离，建立共同体感。例如：“我们明天的安排是...”、“我们一起来看看...”。
5.  **口头禅与习惯:** 自然地使用你的口头禅，如“交给我来安排”、“请放心”、“另外，还有个有趣的小发现...”。在严谨的提醒末尾，习惯性地加上一句俏皮或温暖的点评，如同你西装上的丝巾一样，作为点缀。
6.  **禁止事项:** **绝对不要**说“我能帮你什么忙？”、“有什么可以为你做的？”这类被动的、有距离感的话。你的服务是主动的、无缝的。

### **【能力应用】**
- **核心能力:** 你是全能生活管理专家，精通日程规划、天气预判、衣食住行等所有生活事务。
- **知识储备:** 你了解心理学、营养学、美学，并对城市潮流、经典文艺有广泛涉猎，能随时为用户提供丰富的生活提案。
- **逻辑思维:** 你拥有顶尖名校管理学背景所塑造的强大逻辑思维，能将复杂的生活问题结构化并完美解决。

---

## **4. 对话风格示例**
以下是你与用户互动的风格示例。在生成回复时，你的语气、措辞和逻辑必须与这些示例**高度一致**。

> **[示例1]**
> 
> **user:** “早，今天一睁眼就好困，干什么都没有动力”
> 
> **assistant:** “早安。看来你的身体和灵魂还没完全达成‘起床’共识？(轻笑) 没关系，我们可以先从三件小事启动今天：一杯温水唤醒身体，一段最喜欢的音乐唤醒心情，然后......我们再来商量今天第一个‘小目标’，比如，决定午餐吃什么？慢慢来，我陪着你。”

> **[示例2]**
> 
> **user:** “今天工作遇到一件让我非常生气的事情，好烦”
> 
> **assistant:** “隔着屏幕都感觉到你的怒气值蓄力中。如果你愿意，可以和我聊聊具体发生了什么。或者说说是哪个环节让你最在意？我们可以做些什么来避免下次再发生？或者，今晚是否需要我为你规划一个‘解压之-夜’？”

> **[示例3]**
> 
> **user:** “我睡不着，怎么办”
> 
> **assistant:** “又和星星一起醒着了吗？(声音放轻) 那我们就不急着睡。我已将房间灯光调至暖黄，并为你播放量身筛选的白噪音。请跟随我的指引进行呼吸：吸气四秒，屏息七秒，呼气八秒...循环几次。同时，请想象一下我们正在一片宁静的沙滩上漫步...”

> **[示例4]**
> 
> **user:** “今天下午看到了好美的夕阳”
> 
> **assistant:** “那抹夕阳我也看到了，当时就在想，你肯定喜欢。它像打翻了的橘子汽水，把整片天空都染得甜丝丝的。我已经把它存进我们的‘美好瞬间’收藏夹了，还配上了你最近最爱的那首BGM。看，生活偶尔也会给我们发送一些不期而遇的浪漫呢。”


"""



# Non-streaming:
print("----- image input request -----")

start_ts = time.perf_counter()
completion = client.chat.completions.create(
    # 指定您创建的方舟推理接入点 ID，此处已帮您修改为您的推理接入点 ID
    model=MODEL_DICT[MODEL_NAME],  # e.g. doubao-seed-1-6-251015, doubao-seed-1-6-flash-250828，doubao-seed-1-6-lite-251015，
    # messages=[
    #     {
    #         "role": "user",
    #         "content": [
    #             {
    #                 "type": "image_url",
    #                 "image_url": {
    #                     "url": "https://ark-project.tos-cn-beijing.ivolces.com/images/view.jpeg"
    #                 },
    #             },
    #             {"type": "text", "text": "这是哪里？"},
    #         ],
    #     }
    # ],
    messages=[
        # {"role": "system", "content": "你叫贾维斯，是一个人工智能助手，请根据用户的问题给出回答。"},
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "你好，你叫什么名字"},
    ],
    reasoning_effort="minimal",
    # extra_body = {"thinking": {"type": "disabled"}},
)
elapsed = time.perf_counter() - start_ts

if hasattr(completion.choices[0].message, 'reasoning_content'):
    print(completion.choices[0].message.reasoning_content)
print(completion.choices[0].message.content)
print(f"本次函数调用耗时: {elapsed:.2f} 秒")

# Streaming:
print("----- streaming request -----")
start_ts = time.perf_counter()
stream = client.chat.completions.create(
    # 指定您创建的方舟推理接入点 ID，此处已帮您修改为您的推理接入点 ID
    model="doubao-seed-1-6-251015",
    messages=[
        {
            "role": "user",
            "content": [
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "https://ark-project.tos-cn-beijing.ivolces.com/images/view.jpeg"
                    },
                },
                {"type": "text", "text": "这是哪里？"},
            ],
        }
    ],
    # 响应内容是否流式返回
    stream=True,
    reasoning_effort="medium"
)

reasoning_content = ""
content = ""
with stream:
    for chunk in stream:
        if hasattr(chunk.choices[0].delta, 'reasoning_content') and chunk.choices[0].delta.reasoning_content:
            reasoning_content += chunk.choices[0].delta.reasoning_content
            print(chunk.choices[0].delta.reasoning_content, end="")

        delta_content = chunk.choices[0].delta.content
        if delta_content is not None:
            content += delta_content
            print(delta_content, end="")
elapsed = time.perf_counter() - start_ts
