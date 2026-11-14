# 字典演进中的 Arrow 流接收

dictionary-stream 用于合作资料的本地查看和来源保存。整份资料的查看与字节级归档保持原样；新增的连续接收端直接消费分批到达的 Arrow IPC stream，在字典被替换或追加的情况下逐批发布结果，并把业务确认与断线恢复位置衔接起来。

## 运行环境

使用 Node.js 22。依赖已锁定；首次准备执行 `npm ci --ignore-scripts`，正式运行无需下载资料或软件。全部样例使用合成数据与本机来源，不连接生产系统。

```bash
npm run check
npm test
npm run demo
```

`npm run demo` 在临时目录实际读取资料、归档并重新核验摘要，然后对 `fixtures/session.arrow` 跑完整接收流程：任意字节切分、字典演进、空数组与空值、下游停住、背压恢复、从流起点重播。退出时清理临时内容；正常演示在数秒内结束。

## 资料操作

```bash
npm start -- inspect fixtures/observations.arrow
npm start -- capture fixtures/observations.arrow .local/archive
npm start -- list .local/archive
```

`capture` 只表示来源字节已保存，不声称领域流程已自动完成。已有查看功能只展示现有资料，不替代业务接纳判断。`scripts/create-fixtures.js` 用于维护者重建合成资料；运行与测试直接读取已提交的资料。

## 连续接收

```js
import { StreamIntake, RecoveryLedger } from './src/stream.js';

const ledger = await RecoveryLedger.load('.local/recovery.json');
const intake = new StreamIntake({
  ledger,
  maxInflightBatches: 2,
  onRecord(record) {
    // record.start / record.end 是来源字节位置，record.dictionaries 是本批所用字典版本
    publish(record.preview());
    record.acknowledge({ by: '业务确认单号' });
  },
});

// source 只需是异步可迭代的字节来源（TCP 分块、Readable、fetch body 均可）。
await intake.ingest(source);
await ledger.save('.local/recovery.json');
```

处理规则：

- **帧边界**：`src/framing.js` 把流按 IPC 长度前缀切开。现代格式要求元数据与正文都按 8 字节对齐，尾标记是 `-1` 加长度 `0`；旧格式（4 字节直接给长度、`0` 作尾标记）也接受，但一帧内不混用。只有整帧到齐才解码，未到齐时缓冲只保留当前帧可能的前缀；尾标记之后再来的字节、缺少尾标记的断流、超过上限的长度都会显式报错。
- **字典版本**：`src/decode.js` 保存 schema 和字典版本。字典批次到达即生效，`isDelta` 为真时追加，否则整份替换；批次解码时取当时的字典快照，之后字典再变也不会改变已发布批次的值。不同字段可以引用同一字典编号。
- **消费确认**：每个批次在 `onRecord` 中拿到 `start`/`end`、元数据段与正文段、行列值，以及所用字典版本。`record.acknowledge()` 才把该批次写入恢复账本；重复确认幂等。
- **背压**：已发布未确认批次数达到 `maxInflightBatches` 时，接收端停止向来源要字节，直到最早一批被确认。接收端不会替慢消费者缓存后续原始块。
- **恢复**：合作方只允许从流起点重播。重播时账本中的批次按「帧起点 + 元数据长度 + 正文长度」为键跳过，并校验元数据与正文摘要一致；未确认的批次不在账本中，会重新发布。`summary().resumeHint` 给出 `partnerReplayFrom: 0`、第一个未确认批次的起点和连续确认到的位置。

`npm run demo` 会逐项打印这些结论，并以非零退出码报告任何一项不成立。

## 目录约定

- `src/domain.js`：现有领域读取和查看接口。
- `src/archive.js`：内容对象、来源回执与完整性检查。
- `src/framing.js`：IPC 字节流的增量帧解析与边界校验。
- `src/decode.js`：schema、字典版本与记录批次解码。
- `src/stream.js`：接收、发布、确认、背压与恢复账本。
- `src/demo.js`：演示与验收用场景，`src/session-stream.js`：演示流的生成。
- `fixtures/`：本地资料与领域配置；`session.arrow` 是演示用的多批次字典流，`stream-plan.json` 记录切点和预期帧位置。
- `test/`：已有行为与新增接收行为的运行验证，使用独立临时目录。

## Linux 环境

`Dockerfile` 使用 Node.js 22 镜像。可执行 `docker build -t dictionary-stream:local .` 后运行 `docker run --rm --network none dictionary-stream:local npm test`。测试不访问网络。构建阶段准备依赖，运行时不安装依赖。
