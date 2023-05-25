import { useState, useEffect } from "react";
// import API_KEY from "./keys";

//import CONTEXT_KEY from "./keys";
const CONTEXT_KEY = "a811fc6a6f3c14a0d";
const useSearchAPI = (term) => {
  const [data, setData] = useState(null);
  useEffect(() => {
    const fetchData = async () => {
      fetch(
          `http://localhost:45555/search?query=${term}`
//     `https://www.googleapis.com/customsearch/v1?key=${API_KEY}&cx=${CONTEXT_KEY}&q=${term}`
      )
        .then((response) => response.json())
        .then((result) => {
          console.log(result);
          setData(result);
        });
    };

    fetchData();
  }, [term]);

  return { data };
};
export default useSearchAPI;
