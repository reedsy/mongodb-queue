function timeout(millis) {
  return new Promise((resolve) => setTimeout(resolve, millis));
}

module.exports = {
  timeout,
};
