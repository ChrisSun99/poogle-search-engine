import React from "react";
import "./Home.css";
import Search from "../components/Search";
import logo from '../assets/poogle.png';

function Home() {
  return (
    <div className="home">
      <div className="home__body">
        <img
          src={logo}
          alt="Img"
        />
        <div className="home__inputContainer">
          <Search/>
        </div>
      </div>
    </div>
  );
}

export default Home;
