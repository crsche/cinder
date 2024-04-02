import * as React from "react";

import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export function ListItem() {
  return (
    <Card className="w-full">
      <div style={{ height: "18px" }}></div>
      <CardContent>
        <div className="grid w-full items-center gap-4">
          <div className="flex flex-col space-y-1.5">
            <Label>College Name</Label>
          </div>
          <div className="flex flex-col space-y-1.5">
            <Label>More information...</Label>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
