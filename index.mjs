import { Groq } from 'groq-sdk';
import stream from "stream";
import dotenv from 'dotenv'
import csv from 'csv-parser';
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { PutCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

dotenv.config();
const groq = new Groq({ apiKey: process.env.API_KEY });

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);


export const handler = async (event) => {

    try {
        if (!event.body) {
            throw new Error("No File Uploaded")
        }

        const fileContent = Buffer.from(event.body, event.isBase64Encoded ? 'base64' : 'utf8');
        const csvData = await parseCSV(fileContent);
        const prompt = createPromptFromCSV(csvData);
        const summary = await getGroqSummary(prompt);
        await uploadSummaryToDynamoDB(summary);


        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              message: 'CSV processed successfully',
              summary: summary,
            })
        };

    }catch(error){
        return {
            statusCode: 400,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ error: error.message })
        };
    }
};

async function uploadSummaryToDynamoDB(summaryArray) {
    const item = {
        summary
    };

    return docClient.send(new PutCommand({
        TableName: "CSV_Summaries",
        Item: item
    }));
}

function parseCSV(fileContent) {
    return new Promise((resolve, reject) => {
      const results = [];
      const bufferStream = new stream.PassThrough();
      bufferStream.end(fileContent);
      
      bufferStream
        .pipe(csv())
        .on('data', (data) => results.push(data))
        .on('end', () => resolve(results))
        .on('error', (error) => reject(error));
    });
  }

  function createPromptFromCSV(csvData) {
    const rows = csvData.map(row => 
      JSON.stringify(row)
    ).join('\n');
    
    return `
        
      I have a CSV file that I converted into a string format. I want you to process the rows and return a summary in a json format.
      CSV ROWS:
      ${rows}
    `;
  }

  async function getGroqSummary(prompt) {
    const chatCompletion = await groq.chat.completions.create({
      messages: [
        {
            role: "system",
            content: "You are a CSV summarizing agent. You must convert provided rows into a json format"
        },
        {
          role: "user",
          content: prompt
        }
      ],
      model: "llama-3.3-70b-versatile",
      response_format: { type: "json_object" }
    });
    
    try {
      return JSON.parse(chatCompletion.choices[0]?.message?.content || '{}');
    } catch (e) {
      console.error('Failed to parse Groq response:', e);
      return { error: 'Failed to parse AI response' };
    }
  }
