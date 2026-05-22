export const TOPICS = {
  CLICKED_VALUE: "drivado.search.clicked.topic",
  EXTERNAL_DATA: "drivado.search.backup.topic",
  ORS_RESPONSE: "drivado.search.ors.topic"
};

























// const TOPIC_DELIMETER = ".";

// const topicFactory = {
//   all: () => ["drivado"],
//   search: {
//     all: () => ["search"],
//     clicked: () => [topicFactory.all(), topicFactory.search.all(), "clicked"],
//     clickedById: (id) => [
//       topicFactory.all(),
//       topicFactory.search.all(),
//       "clicked",
//       id,
//     ],
//     backup: () => [topicFactory.all(), topicFactory.search.all(), "backup"],
//   },
// };

// function generateTopicString(topicArray, delimeter) {
//   const tempArray = [...topicArray, "topic"];
//   return tempArray.join(delimeter);
// }

// console.log(
//   generateTopicString(topicFactory.search.clicked(), TOPIC_DELIMETER),
// );
// console.log(generateTopicString(topicFactory.search.backup(), TOPIC_DELIMETER));
// console.log(
//   generateTopicString(
//     topicFactory.search.clickedById("123456789"),
//     TOPIC_DELIMETER,
//   ),
// );

// module.exports.topicsRahul = topicsRahul;
