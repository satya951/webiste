// Define a function to show the specified file content
function showFile(file) {
  // Hide all File-content
  document.querySelectorAll('.File-content').forEach(function(el) {
      el.classList.remove('active');
  });
  // Show the selected File-content
  document.getElementById('File' + file).classList.add('active');
}

// Define a function to redirect to the desired URL
function redirectToUrl(url) {
  // Redirect users to the desired URL
  window.location.href = url;
}
