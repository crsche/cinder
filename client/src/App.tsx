"use client";

import * as React from "react";
import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { ThemeProvider } from "@/components/theme-provider";
import { SendHorizonal } from "lucide-react";

import "../app/globals.css";
import "./App.css";
import { ModeToggle } from "./components/mode-toggle";

function App() {
  // const [isOpen, setIsOpen] = React.useState(false);

  return (
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      {/* <ModeToggle/> */}

      <Sheet open={true} modal={false}>
        <SheetContent side={"left"}>
          <SheetHeader>
            <SheetTitle>Search for Colleges</SheetTitle>
            <SheetDescription>With SQL</SheetDescription>
          </SheetHeader>
          <div className="grid gap-4 py-4">
            <div className="grid grid-cols-4 items-center gap-4">
              <Input id="query" placeholder="SQL Query" className="col-span-3" autoComplete="off"/>
              <Button type="submit">
                <SendHorizonal className="h-4 w-4"/>
              </Button>
            </div>
          </div>
        </SheetContent>
      </Sheet>
    </ThemeProvider>
  );
}

export default App;
